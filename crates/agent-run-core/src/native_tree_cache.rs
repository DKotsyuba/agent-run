//! Freeze/thaw lifecycle for native Codex directory caches in retained homes.
//!
//! Idle retained homes each hold a private duplicate of the same native
//! directory caches — the generated `skills/.system` tree, the
//! remote-downloaded plugin parents under `plugins/cache/<market>/<plugin>`
//! marked by the native store's `.codex-remote-plugin-install.json` record,
//! and the native curated plugin marketplace clone's working-tree
//! `.tmp/plugins/plugins` plus its immutable Git pack directory
//! `.tmp/plugins/.git/objects/pack`. The clone's other Git state (HEAD,
//! refs, index, logs, config, locks), `.agents`, root files and the
//! `.tmp/plugins.sha`/`.tmp/plugins.sync.lock` siblings always stay private.
//! This unit removes those duplicates without changing what the native SDK
//! sees or does: while a home is caller-proven idle, `freeze` captures the
//! verified private tree into the shared store's existing content-addressed
//! tree namespace and replaces the private root with one exact whole-root
//! symlink; `thaw` restores an independent writable real tree — APFS
//! clone-backed, never an external hardlink — before any harness execution
//! that could mutate the cache (resume, retry, account switch, probes).
//!
//! There is deliberately no second content-addressed store: capture builds a
//! bounded managed-snapshot staging copy inside the operation backup, the
//! existing `shared_assets::import_shared_tree` publisher (canonical blobs
//! plus `trees/<scope>/<manifest-sha>` readonly trees) imports it, and every
//! later check reuses `verify_shared_tree`, `shared_tree_root`, and
//! `shared_tree_blob_names`. An operation's `op.json` record — written into
//! the home before import runs — is the durable pin that keeps a
//! published-but-not-yet-linked tree visible to `scan_refs`, so no
//! untracked-reference window survives a crash, and the home-link install
//! itself runs under one global `SharedStoreLock` hold. The import's own
//! internal lock hold is never nested inside ours.
//!
//! `<home>/.agent-run-native-<uuid>/` is the operation backup: `op.json`
//! plus, while an operation runs, the staged capture (freeze), the moved
//! original (freeze), or the staged clone (thaw). `recover` completes or
//! rolls back from these records without losing originals.
//!
//! `<scope>` is the caller-supplied trusted account/connection
//! compatibility domain (64 lowercase hex); it is never derived from a name
//! or label, so one account's native cache is never restored for another.
//! Excluded forever: managed personal plugin parents
//! (`plugins/cache/personal/...`), anything overlapping the home's frozen
//! runtime index (`roots`, `files`, `links`), local-marketplace parents
//! without a valid remote marker, credentials, config, history, and any tree
//! holding a symlink or special entry — those shapes are refused or skipped
//! with the original left untouched, never guessed disposable.

use crate::fs::{self, Dir, EntryType};
use agent_run_domain::{Error, Result, canonical, error::invalid};
use agent_run_platform::{
    shared_assets::{self, MAX_TREE_MANIFEST_BYTES, SharedStoreLock, SharedTreeRef, is_scope},
    snapshot_tree::{RUNTIME_SNAPSHOT_INDEX, SNAPSHOT_MANIFEST},
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::Digest;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Native remote-plugin install marker filename the eligibility check reads.
const REMOTE_MARKER: &str = ".codex-remote-plugin-install.json";
/// Prefix of native-cache operation backup directories inside one home.
const BACKUP_PREFIX: &str = ".agent-run-native-";
/// Operation record filename inside one backup directory.
const OP_RECORD: &str = "op.json";
/// Name of the staged capture subtree inside one freeze backup.
const STAGING: &str = "staging";
/// Name of the staged clone subtree inside one thaw backup.
const CLONE: &str = "tree";
/// Name a proven freeze original takes inside its backup once its removal
/// may begin, so a crash mid-removal never leaves an unprovable original.
const DISCARD: &str = "discard";
/// Upper bound for one operation record, marker, or runtime index read.
/// Shared-tree manifests use [`MAX_TREE_MANIFEST_BYTES`] instead.
const MAX_METADATA: usize = 64 * 1024;
/// Per-file payload bound, matching the shared store's streaming bound; it
/// admits a native Git pack (measured 24 MB) and refuses anything larger.
const MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
/// Maximum manifest entries one native tree may describe, matching the
/// shared store (the measured curated mirror holds 7732 entries).
const MAX_TREE_ENTRIES: usize = 16_384;
/// Maximum aggregate payload bytes one native tree may describe.
const MAX_TREE_BYTES: u64 = 128 * 1024 * 1024;
/// Maximum directory depth beneath one captured root (the measured curated
/// mirror reaches depth 10).
const MAX_DEPTH: u8 = 16;
/// Home-relative working-tree plugins directory of the native curated
/// plugin marketplace clone.
pub const CURATED_MIRROR_ROOT: &str = ".tmp/plugins/plugins";
/// Home-relative Git pack directory of the same native curated clone.
pub const CURATED_PACK_ROOT: &str = ".tmp/plugins/.git/objects/pack";
/// Home-relative Git directory whose presence proves the curated clone.
const CURATED_GIT_DIR: &str = ".tmp/plugins/.git";
/// Git pack file extensions a frozen pack directory may hold.
const PACK_EXTENSIONS: [&str; 7] = ["pack", "idx", "rev", "bitmap", "keep", "mtimes", "promisor"];
/// Wall-clock budget for one capture or census pass.
const TIME_BUDGET: Duration = Duration::from_secs(10);
/// Upper bound for one component of a cache root path.
const MAX_COMPONENT: usize = 128;
/// Upper bound of plugin parents one census pass examines per home.
const MAX_CENSUS_PARENTS: usize = 4096;
/// Upper bound of referenced blob paths one census pass collects before it
/// reports itself incomplete for the caller to page.
const MAX_CENSUS_BLOBS: usize = 200_000;
/// Upper bound of operation backups one census pass reads per home.
const MAX_CENSUS_BACKUPS: usize = 32;
/// Streaming chunk for hashing payloads.
const STREAM_CHUNK: usize = 64 * 1024;

/// The native cache kinds this unit manages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeCacheKind {
    /// The generated `skills/.system` skills tree. Native discovery reads
    /// through a whole-root link, and an SDK marker upgrade replaces the
    /// private link with a freshly extracted private directory while the
    /// shared tree stays untouched — so this root may keep its link across
    /// harness runs and is re-frozen on the next confirmed idle pass.
    SystemSkills,
    /// One remote-downloaded plugin parent
    /// `plugins/cache/<market>/<plugin>` carrying a valid
    /// `.codex-remote-plugin-install.json` marker. The native store writes
    /// version updates into the parent itself, so this root must be
    /// `thaw`ed into a real writable tree before every native invocation.
    RemotePluginParent,
    /// The working-tree plugins directory [`CURATED_MIRROR_ROOT`] of the
    /// native curated marketplace clone, frozen only when the clone's
    /// `.git` is a real directory. The native startup sync fetches into the
    /// private `.git`, stages a replacement repository and activates it by
    /// renaming the whole clone, and the plugin manager copies installed
    /// payloads out of this tree, so it is `thaw`ed before every native
    /// invocation exactly like a remote plugin parent.
    CuratedMirror,
    /// The Git pack directory [`CURATED_PACK_ROOT`] of the same clone,
    /// frozen only when it holds nothing but regular `pack-<hash>.<ext>`
    /// files. Pack files are immutable and byte-identical across homes
    /// cloned from the same upstream state; HEAD, refs, index, logs, config
    /// and locks stay private and untouched. `thaw`ed before every native
    /// invocation so fetch and repack keep native behavior.
    CuratedPacks,
}

/// The outcome of one `freeze` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FreezeOutcome {
    /// The private root was captured, imported, and replaced by its exact
    /// shared-tree link this call; the value is the tree reference.
    Frozen(SharedTreeRef),
    /// The root was already an exact controlled link; its tree was verified
    /// and nothing was changed.
    AlreadyFrozen(SharedTreeRef),
    /// The root is not a remote-managed native cache — its marker is absent
    /// or invalid — and was left completely unchanged.
    SkippedUnchanged,
}

/// One census pass over a caller-supplied page of extant retained homes.
///
/// `trees` are the shared-tree targets the scanned homes' controlled cache
/// links and pending operation backups reference; `blobs` are the
/// store-relative canonical blob paths those trees hardlink. Both are
/// derived from exact controlled-link text and manifest reads only — the
/// census re-hashes no payload; the shared collector later proves each
/// pinned tree's manifest and topology under the store lock.
///
/// `complete` is true only when every home, link, backup record, and
/// manifest on the page was read successfully within the bounds — a
/// collector must treat `complete == false` as "evidence is partial", never
/// as "nothing is referenced". `homes` is the explicit progress marker: it
/// counts homes this pass finished examining, so a caller paging a larger
/// retained-home set resumes at that offset and merges pages with
/// [`NativeRefScan::merge`] instead of re-scanning.
#[derive(Debug, Default, Serialize)]
pub struct NativeRefScan {
    /// True only when the pass read every candidate without error or bound.
    pub complete: bool,
    /// Referenced tree identities keyed by `<scope>/<manifest-sha>`, whose
    /// values are the [`SharedTreeRef`] identities themselves.
    pub trees: BTreeMap<String, SharedTreeRef>,
    /// Store-relative blob paths backing the referenced trees.
    pub blobs: BTreeSet<PathBuf>,
    /// Homes this pass finished examining, including ones that failed to
    /// open; the resume offset for a paged caller.
    pub homes: usize,
    /// Operation backup records read.
    pub backups: usize,
}

impl NativeRefScan {
    /// Merges one later page into this pass.
    ///
    /// Reference sets and counters union; `complete` stays true only when
    /// every merged page completed. A caller paging a retained-home set
    /// merges pages in order and resumes at the summed `homes` offset
    /// whenever an earlier page reports `complete == false`.
    pub fn merge(&mut self, page: NativeRefScan) {
        self.complete &= page.complete;
        self.trees.extend(page.trees);
        self.blobs.extend(page.blobs);
        self.homes += page.homes;
        self.backups += page.backups;
    }
}

/// Returns whether `value` is safe path material for one root component:
/// nonempty, bounded, and restricted to native identity characters.
fn safe_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_COMPONENT
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.+-".contains(&byte))
}

/// Classifies one home-relative root as a managed native cache kind.
///
/// `skills/.system` classifies as [`NativeCacheKind::SystemSkills`], exactly
/// [`CURATED_MIRROR_ROOT`] as [`NativeCacheKind::CuratedMirror`] and exactly
/// [`CURATED_PACK_ROOT`] as [`NativeCacheKind::CuratedPacks`] — never any
/// other path in or around the curated clone; exactly
/// `plugins/cache/<market>/<plugin>` with both trailing components safe and
/// `market` not `personal` classifies as
/// [`NativeCacheKind::RemotePluginParent`] — classification alone does not
/// prove remote provenance; `freeze` additionally requires the parent's
/// native remote-install marker. Everything else — shallower or deeper
/// paths, the managed `personal` marketplace, unsafe components,
/// `plugins/data` — is refused: this unit never treats an unclassified
/// directory as disposable cache.
pub fn classify(root_key: &str) -> Result<NativeCacheKind> {
    match root_key {
        "skills/.system" => return Ok(NativeCacheKind::SystemSkills),
        CURATED_MIRROR_ROOT => return Ok(NativeCacheKind::CuratedMirror),
        CURATED_PACK_ROOT => return Ok(NativeCacheKind::CuratedPacks),
        _ => {}
    }
    let rest = root_key
        .strip_prefix("plugins/cache/")
        .ok_or_else(|| invalid("native cache root is not a supported native cache path"))?;
    if rest.matches('/').count() != 1 {
        return Err(invalid(
            "native cache root must be one plugin parent below plugins/cache",
        ));
    }
    let mut parts = rest.split('/');
    let market = parts.next().expect("one separator splits two parts");
    let plugin = parts.next().expect("one separator splits two parts");
    if market == "personal" {
        return Err(invalid(
            "managed personal plugin parents are covered by managed parent views",
        ));
    }
    if !safe_component(market) || !safe_component(plugin) {
        return Err(invalid(
            "native cache root components must be safe identity material",
        ));
    }
    Ok(NativeCacheKind::RemotePluginParent)
}

/// Reads and validates one plugin parent's native remote-install marker.
///
/// The marker must be one readable regular JSON file directly inside the
/// parent whose `schema_version` is exactly 1 (number or string) and whose
/// `remote_plugin_id` is a nonempty string, matching the native store's
/// schema-1 contract. A missing, unreadable, or invalid marker is `false`;
/// the caller skips such parents unchanged instead of freezing them.
fn valid_remote_marker(home: &Dir, root_key: &str) -> Result<bool> {
    let marker = Path::new(root_key).join(REMOTE_MARKER);
    let Some(raw) = home.optional(&marker, MAX_METADATA)? else {
        return Ok(false);
    };
    let Ok(document) = serde_json::from_slice::<Value>(&raw) else {
        return Ok(false);
    };
    let schema = match &document["schema_version"] {
        Value::Number(number) => number.as_u64() == Some(1),
        Value::String(text) => text == "1",
        _ => false,
    };
    let remote_id = document["remote_plugin_id"].as_str();
    Ok(schema && remote_id.is_some_and(|id| !id.is_empty()))
}

/// Returns whether one curated root has the native shape this unit freezes.
///
/// Both curated kinds require the clone's `.git` to be a real directory
/// (a gitfile, link or absent `.git` is not the native curated clone). The
/// pack kind additionally requires a nonempty directory of regular
/// `pack-<40 or 64 hex>.<ext>` files with a known Git pack extension —
/// a `multi-pack-index`, a temporary pack or any other shape is left
/// private. `false` means "skip unchanged"; only unexpected I/O errors
/// propagate.
fn curated_shape(home: &Dir, kind: NativeCacheKind) -> Result<bool> {
    match home.entry_type(Path::new(CURATED_GIT_DIR)) {
        Ok(EntryType::Directory) => {}
        Ok(_) => return Ok(false),
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    }
    if kind != NativeCacheKind::CuratedPacks {
        return Ok(true);
    }
    let names = home.list(Some(Path::new(CURATED_PACK_ROOT)))?;
    if names.is_empty() {
        return Ok(false);
    }
    for name in names {
        let Some(text) = name.to_str() else {
            return Ok(false);
        };
        let Some((hash, extension)) = text
            .strip_prefix("pack-")
            .and_then(|rest| rest.split_once('.'))
        else {
            return Ok(false);
        };
        let hex = hash
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
        if !matches!(hash.len(), 40 | 64) || !hex || !PACK_EXTENSIONS.contains(&extension) {
            return Ok(false);
        }
        if home.entry_type(&Path::new(CURATED_PACK_ROOT).join(text))? != EntryType::File {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Returns whether `prefix` is a component-wise prefix of `path`.
fn component_prefix(prefix: &[&str], path: &[&str]) -> bool {
    prefix.len() <= path.len() && prefix.iter().zip(path).all(|(a, b)| a == b)
}

/// Refuses to operate on any root overlapping the home's frozen runtime
/// index.
///
/// The index — when present — is read as exact bytes and parsed; a malformed
/// index is an explicit refusal, never a guess. The root is refused when it
/// equals, is an ancestor of, or is a descendant of any indexed managed
/// root, flat file, or credential link, so a native freeze can never replace
/// or shadow an asset the managed-asset verifiers own.
fn refuse_index_overlap(home: &Dir, root_key: &str) -> Result<()> {
    let Some(raw) = home.optional(Path::new(RUNTIME_SNAPSHOT_INDEX), MAX_METADATA)? else {
        return Ok(());
    };
    let document: Value =
        serde_json::from_slice(&raw).map_err(|_| invalid("runtime snapshot index is malformed"))?;
    let root_parts: Vec<&str> = root_key.split('/').collect();
    for key in ["roots", "files", "links"] {
        let Some(entries) = document[key].as_array() else {
            return Err(invalid("runtime snapshot index is malformed"));
        };
        for entry in entries {
            let path = match entry {
                Value::String(path) => path.clone(),
                entry => entry
                    .get("path")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("runtime snapshot index is malformed"))?
                    .to_owned(),
            };
            let parts: Vec<&str> = path.split('/').collect();
            if component_prefix(&parts, &root_parts) || component_prefix(&root_parts, &parts) {
                return Err(invalid(format!(
                    "native cache root {root_key} overlaps the frozen runtime index entry {path}"
                )));
            }
        }
    }
    Ok(())
}

/// Returns the validated canonical store root, refusing aliased paths.
fn validated_store(store_root: &Path) -> Result<PathBuf> {
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

/// Returns whether `value` is a strict 64-lowercase-hex digest.
fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// One captured native tree: validated manifest entries and exact bytes.
struct Capture {
    /// Manifest entries sorted by path, Python-v1 shape.
    entries: Vec<Value>,
    /// Canonical manifest bytes (trailing newline included).
    manifest: Vec<u8>,
    /// SHA-256 of `manifest`.
    manifest_sha256: String,
}

/// Canonical Python-v1 bytes for one manifest document.
fn canonical_manifest(entries: &[Value]) -> Vec<u8> {
    let mut bytes = canonical::dumps(&json!({"snapshot_version": 1, "entries": entries}), true);
    bytes.push(b'\n');
    bytes
}

/// Fails when one entry is not owned by the effective user.
fn require_owner(uid: u32, label: &str) -> Result<()> {
    // SAFETY: geteuid only reads kernel credential state and retains nothing.
    if uid != unsafe { libc::geteuid() } {
        return Err(invalid(format!("native cache {label} has a foreign owner")));
    }
    Ok(())
}

/// Captures one native root's exact content and topology.
///
/// Walks only through no-follow descriptors. Every visited entry must be a
/// real owner-held directory or regular file — a symlink or special entry is
/// an explicit unsupported-shape refusal that leaves the tree untouched.
/// File modes normalize to the store's logical `0o600`/`0o700` pair (owner
/// access and the execution bit are preserved; group/other bits are not
/// native-private state). The walk is bounded by [`MAX_TREE_ENTRIES`],
/// [`MAX_FILE_BYTES`], [`MAX_TREE_BYTES`], [`MAX_DEPTH`], and
/// `TIME_BUDGET`, and its canonical manifest by [`MAX_TREE_MANIFEST_BYTES`];
/// each payload is streamed through a running hash without buffering the
/// whole tree.
fn capture(directory: &Dir, deadline: Instant) -> Result<Capture> {
    let mut entries: Vec<Value> = Vec::new();
    let mut total = 0_u64;
    walk_capture(directory, "", 0, &mut entries, &mut total, deadline)?;
    entries.sort_by_key(|entry| entry["path"].as_str().unwrap_or_default().to_owned());
    if entries.len() > MAX_TREE_ENTRIES {
        return Err(invalid("native cache tree exceeds the entry bound"));
    }
    let manifest = canonical_manifest(&entries);
    if manifest.len() > MAX_TREE_MANIFEST_BYTES {
        return Err(invalid("native cache manifest exceeds the manifest bound"));
    }
    Ok(Capture {
        manifest_sha256: fs::sha256(&manifest),
        manifest,
        entries,
    })
}

/// One recursion level of [`capture`]; `prefix` is the visited subtree path
/// and `total` accumulates the whole capture's payload bytes across every
/// level, so the aggregate bound covers the entire tree rather than one
/// directory.
fn walk_capture(
    directory: &Dir,
    prefix: &str,
    depth: u8,
    entries: &mut Vec<Value>,
    total: &mut u64,
    deadline: Instant,
) -> Result<()> {
    if depth > MAX_DEPTH {
        return Err(invalid("native cache tree exceeds the depth bound"));
    }
    if Instant::now() >= deadline {
        return Err(invalid("native cache capture exceeded its time budget"));
    }
    for name in directory.list(None)? {
        if entries.len() > MAX_TREE_ENTRIES {
            return Err(invalid("native cache tree exceeds the entry bound"));
        }
        let Some(text) = name.to_str() else {
            return Err(invalid("native cache tree holds a non-UTF-8 entry name"));
        };
        if text == SNAPSHOT_MANIFEST {
            return Err(invalid(
                "native cache tree already holds a managed snapshot manifest",
            ));
        }
        let path = if prefix.is_empty() {
            text.to_owned()
        } else {
            format!("{prefix}/{text}")
        };
        let identity = directory.entry(Some(Path::new(text)))?;
        require_owner(identity.uid, "entry")?;
        match identity.kind {
            EntryType::Directory => {
                entries.push(json!({"path": path, "type": "directory"}));
                walk_capture(
                    &directory.subdir(Path::new(text))?,
                    &path,
                    depth + 1,
                    entries,
                    total,
                    deadline,
                )?;
            }
            EntryType::File => {
                let (sha256, length, executable) = hash_entry(directory, text)?;
                *total = total.saturating_add(length);
                if *total > MAX_TREE_BYTES {
                    return Err(invalid("native cache tree exceeds the aggregate bound"));
                }
                entries.push(json!({
                    "path": path,
                    "type": "file",
                    "mode": if executable { 0o700 } else { 0o600 },
                    "bytes": length,
                    "sha256": sha256,
                }));
            }
            kind => {
                return Err(invalid(format!(
                    "native cache tree holds an unsupported entry ({kind:?}): {path}"
                )));
            }
        }
    }
    Ok(())
}

/// Streams one regular file through a running SHA-256 and returns its
/// digest, length, and execution bit, refusing files beyond the payload
/// bound. The hash reads the same no-follow descriptor the identity check
/// classified.
fn hash_entry(directory: &Dir, name: &str) -> Result<(String, u64, bool)> {
    let identity = directory.entry(Some(Path::new(name)))?;
    let mut file = directory.open_file(Path::new(name))?;
    if file.metadata()?.len() > MAX_FILE_BYTES as u64 {
        return Err(invalid("native cache file exceeds the payload bound"));
    }
    let executable = identity.mode & 0o111 != 0;
    let mut hasher = sha2::Sha256::new();
    let mut buffer = vec![0_u8; STREAM_CHUNK];
    let mut length = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        length += read as u64;
        if length > MAX_FILE_BYTES as u64 {
            return Err(invalid("native cache file exceeds the payload bound"));
        }
    }
    Ok((
        canonical::hex_digest(&hasher.finalize()),
        length,
        executable,
    ))
}

/// Builds one managed-snapshot staging copy of the captured tree beneath
/// `staging`, using the platform's APFS-clone snapshot writer per payload so
/// the copy is cheap and inode-independent, then writes the canonical
/// manifest last.
///
/// The copy is disposable input to the store publisher, which streams and
/// durably publishes every payload itself, so it is staged without
/// per-entry durability flushes; a crash leaves a staging subtree recovery
/// drops unconditionally. On a cloning volume the whole `root` hierarchy is
/// cloned in one call and only file modes are normalized to the captured
/// logical modes (the publisher asserts no source directory mode); the
/// publisher's topology and digest checks still refuse any drift from the
/// capture. Otherwise entries are staged one by one, parents first.
fn stage_capture(
    home: &Dir,
    staging: &Path,
    root: &Path,
    source: &Dir,
    capture: &Capture,
) -> Result<()> {
    if home.clone_directory(staging, home, root)? {
        let staged = home.subdir(staging)?;
        staged.permit_owner_write()?;
        for entry in &capture.entries {
            if entry["type"] == "file" {
                let relative = Path::new(entry["path"].as_str().expect("captured paths are str"));
                staged.set_mode(
                    relative,
                    entry["mode"].as_u64().expect("captured mode") as u32,
                )?;
            }
        }
        return home.write(&staging.join(SNAPSHOT_MANIFEST), &capture.manifest, 0o600);
    }
    home.directory(staging)?;
    for entry in &capture.entries {
        let relative = Path::new(entry["path"].as_str().expect("captured paths are str"));
        if entry["type"] == "directory" {
            home.stage_directory(&staging.join(relative))?;
            continue;
        }
        let payload = source.read(relative, MAX_FILE_BYTES)?;
        let mode = entry["mode"].as_u64().expect("captured mode") as u32;
        home.stage_snapshot_file(&staging.join(relative), source, relative, &payload, mode)?;
    }
    home.write(&staging.join(SNAPSHOT_MANIFEST), &capture.manifest, 0o600)
}

/// Parses one home root symlink into the shared-tree reference it names.
///
/// The target text must equal exactly `<store_root>/trees/<scope>/<manifest
/// -sha>` with both trailing components strict digests — the same controlled
/// shape [`shared_assets::shared_tree_root`] derives; anything else — a
/// foreign link, a dangling spelling, a relative path — is refused rather
/// than canonicalized or followed.
fn reference_from_link(home: &Dir, root_key: &str, store_root: &Path) -> Result<SharedTreeRef> {
    let Some(target) = home.read_link(Path::new(root_key))? else {
        return Err(invalid("native cache root is not a controlled link"));
    };
    let suffix = target
        .strip_prefix(store_root.join("trees"))
        .map_err(|_| invalid("native cache root is a foreign link"))?;
    let mut parts = suffix.components();
    let (Some(scope), Some(sha), None) = (
        parts.next().and_then(|part| part.as_os_str().to_str()),
        parts.next().and_then(|part| part.as_os_str().to_str()),
        parts.next(),
    ) else {
        return Err(invalid("native cache root link target is malformed"));
    };
    if !is_scope(scope) || !is_digest(sha) {
        return Err(invalid("native cache root link target is malformed"));
    }
    Ok(SharedTreeRef {
        scope: scope.to_owned(),
        manifest_sha256: sha.to_owned(),
    })
}

/// One durable operation record inside a backup directory.
#[derive(serde::Deserialize, Serialize)]
struct OpRecord {
    /// Record schema version; only `1` is understood.
    op_version: u32,
    /// `"freeze"` or `"thaw"`.
    op: String,
    /// Home-relative cache root the operation switches.
    root: String,
    /// Caller-supplied trusted compatibility scope of the captured tree.
    scope: String,
    /// Manifest digest of the captured tree.
    manifest_sha256: String,
}

/// Returns one record with the given fields.
fn record(op: &str, root_key: &str, reference: &SharedTreeRef) -> OpRecord {
    OpRecord {
        op_version: 1,
        op: op.into(),
        root: root_key.to_owned(),
        scope: reference.scope.clone(),
        manifest_sha256: reference.manifest_sha256.clone(),
    }
}

/// Writes one operation record into a freshly created backup directory.
fn write_record(home: &Dir, backup: &str, record: &OpRecord) -> Result<()> {
    home.directory(Path::new(backup))?;
    let bytes = canonical::dumps(&serde_json::to_value(record)?, true);
    home.write(&Path::new(backup).join(OP_RECORD), &bytes, 0o600)
}

/// Reads one operation record from a backup directory, refusing malformed or
/// unknown shapes.
fn read_record(home: &Dir, backup: &str) -> Result<OpRecord> {
    let raw = home.read(&Path::new(backup).join(OP_RECORD), MAX_METADATA)?;
    let record: OpRecord = serde_json::from_slice(&raw)
        .map_err(|_| invalid("native cache operation record is malformed"))?;
    if record.op_version != 1
        || !matches!(record.op.as_str(), "freeze" | "thaw")
        || classify(&record.root).is_err()
        || !is_scope(&record.scope)
        || !is_digest(&record.manifest_sha256)
    {
        return Err(invalid("native cache operation record is malformed"));
    }
    Ok(record)
}

/// Empties one owned disposable directory through live descriptors, leaving
/// the directory itself for the caller; unexpected entry kinds abort.
///
/// Only disposable subtrees reach here — staging copies, staged clones and
/// proven originals already renamed to [`DISCARD`] — so entries are
/// unlinked without per-entry durability flushes; the caller's final
/// synced removal of the emptied directory is the barrier, and a crash
/// mid-removal leaves a subtree recovery removes unconditionally.
fn remove_tree(directory: &Dir) -> Result<()> {
    directory.permit_owner_write()?;
    for name in directory.list(None)? {
        let relative = PathBuf::from(&name);
        match directory.entry_type(&relative)? {
            EntryType::Directory => {
                remove_tree(&directory.subdir(&relative)?)?;
                directory.discard_directory(&relative)?;
            }
            EntryType::File => {
                directory.discard(&relative)?;
            }
            kind => {
                return Err(invalid(format!(
                    "native cache backup holds an unexpected entry: {kind:?}"
                )));
            }
        }
    }
    Ok(())
}

/// Removes one named subtree inside a backup directory when present.
fn remove_backup_child(home: &Dir, backup: &str, name: &str) -> Result<()> {
    let child = Path::new(backup).join(name);
    match home.entry_type(&child) {
        Ok(EntryType::Directory) => {
            let sub = home.subdir(&child);
            remove_tree(&sub?)?;
            let rm = home.remove_directory(&child);
            rm?;
        }
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(kind) => {
            return Err(invalid(format!(
                "backup holds an unexpected {name} shape: {kind:?}"
            )));
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

/// Removes now-empty backup intermediates below one removed subtree.
fn prune_backup_parents(home: &Dir, removed: &Path, base: &Path) -> Result<()> {
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

/// Removes one freeze backup whose moved original still hashes to the
/// recorded manifest, plus the disposable staged capture.
///
/// The proven original is first renamed to [`DISCARD`] in one synced step,
/// and only then emptied: a crash mid-removal leaves a `discard` subtree a
/// later pass removes without re-proving, never a partial original that
/// could no longer be proven and would block recovery.
fn discard_freeze_backup(home: &Dir, backup: &str, record: &OpRecord) -> Result<()> {
    remove_backup_child(home, backup, STAGING)?;
    remove_backup_child(home, backup, DISCARD)?;
    let staged = Path::new(backup).join(&record.root);
    match home.entry_type(&staged) {
        Ok(EntryType::Directory) => {
            // Each helper enumerates through a fresh handle: a no-follow
            // directory handle's listing is single-shot (its duplicate
            // shares the enumeration offset), so the proof walk and the
            // removal must not share one.
            let capture = capture(&home.subdir(&staged)?, Instant::now() + TIME_BUDGET)?;
            if capture.manifest_sha256 != record.manifest_sha256 {
                return Err(invalid(format!(
                    "backup for {} cannot be proven to hold the recorded tree; left in place",
                    record.root
                )));
            }
            let discard = Path::new(backup).join(DISCARD);
            home.rename_entry_no_replace(&staged, &discard)?;
            prune_backup_parents(home, &staged, Path::new(backup))?;
            remove_backup_child(home, backup, DISCARD)?;
        }
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        entry => {
            return Err(invalid(format!(
                "backup for {} has an unexpected shape: {entry:?}",
                record.root
            )));
        }
    }
    finish_backup_removal(home, backup)
}

/// Removes one thaw backup's staged clone and record directory.
fn discard_thaw_backup(home: &Dir, backup: &str) -> Result<()> {
    remove_backup_child(home, backup, CLONE)?;
    finish_backup_removal(home, backup)
}

/// Removes the backup directory itself once only its record remains.
fn finish_backup_removal(home: &Dir, backup: &str) -> Result<()> {
    let names = home.list(Some(Path::new(backup)))?;
    if names.len() != 1 || names[0].to_str() != Some(OP_RECORD) {
        return Err(invalid(format!(
            "backup {backup} holds unexpected entries; left in place"
        )));
    }
    home.remove(&Path::new(backup).join(OP_RECORD))?;
    home.remove_directory(Path::new(backup))
}

/// Freezes one caller-proven-idle native cache root into the shared store.
///
/// `store_root` is the canonical shared-store root (`shared-assets/v1`),
/// `home_path` the retained runtime home, `root_key` one supported native
/// cache root ([`classify`]), and `scope` the caller-supplied trusted
/// account/connection compatibility domain (64 lowercase hex). The home must
/// be quiescent — no native process may hold or mutate the root.
///
/// A remote plugin parent without a valid native remote-install marker, or
/// a curated root outside its native clone shape, is returned as
/// [`FreezeOutcome::SkippedUnchanged`] without touching anything.
/// Otherwise the private tree is captured with exact content and topology
/// and — after the operation record pins the intent inside the home —
/// either the store already holds that manifest's tree and it fully
/// verifies, or a managed-snapshot staging copy is imported through the
/// existing shared-tree publisher and verified; then — under one global
/// `SharedStoreLock` hold —
/// the original moves into its `.agent-run-native-<uuid>` backup and the
/// root becomes one exact whole-tree symlink, after which the backup is
/// removed only once it still hashes to the same manifest. An
/// already-frozen root is verified and returned idempotently; a foreign
/// link, indexed overlap, unsupported shape, or changed source is an
/// explicit error with the original preserved.
pub fn freeze(
    store_root: &Path,
    home_path: &Path,
    root_key: &str,
    scope: &str,
) -> Result<FreezeOutcome> {
    let kind = classify(root_key)?;
    if !is_scope(scope) {
        return Err(invalid(
            "native cache scope must be 64 lowercase hexadecimal digits",
        ));
    }
    let root = validated_store(store_root)?;
    let home_path = home_path
        .canonicalize()
        .map_err(|_| invalid("runtime home must be an existing real directory"))?;
    let home = Dir::open(&home_path)?;
    refuse_index_overlap(&home, root_key)?;
    match home.entry_type(Path::new(root_key)) {
        Ok(EntryType::Symlink) => {
            let reference = reference_from_link(&home, root_key, &root)?;
            shared_assets::verify_shared_tree(&root, &reference)?;
            Ok(FreezeOutcome::AlreadyFrozen(reference))
        }
        Ok(EntryType::Directory) => {
            let eligible = match kind {
                NativeCacheKind::SystemSkills => true,
                NativeCacheKind::RemotePluginParent => valid_remote_marker(&home, root_key)?,
                NativeCacheKind::CuratedMirror | NativeCacheKind::CuratedPacks => {
                    curated_shape(&home, kind)?
                }
            };
            if !eligible {
                return Ok(FreezeOutcome::SkippedUnchanged);
            }
            freeze_directory(&root, &home_path, &home, root_key, scope)
        }
        Ok(kind) => Err(invalid(format!(
            "native cache root has an unsupported shape: {kind:?}"
        ))),
        Err(error) => Err(error),
    }
}

/// Captures and switches one real private cache root, described by
/// `freeze`; the caller has already classified, overlap-checked, and
/// marker-checked the root.
fn freeze_directory(
    root: &Path,
    home_path: &Path,
    home: &Dir,
    root_key: &str,
    scope: &str,
) -> Result<FreezeOutcome> {
    let source = home.subdir(Path::new(root_key))?;
    let deadline = Instant::now() + TIME_BUDGET;
    let captured = capture(&source, deadline)?;
    let reference = SharedTreeRef {
        scope: scope.to_owned(),
        manifest_sha256: captured.manifest_sha256.clone(),
    };
    // The record pins the tree for census and recovery before the import
    // publishes anything, so a crash cannot strand an unpinned store object.
    let backup = format!("{BACKUP_PREFIX}{}", uuid::Uuid::new_v4().simple());
    write_record(home, &backup, &record("freeze", root_key, &reference))?;
    // A store tree already published under the captured manifest digest is
    // the same content by construction; once it fully verifies, no staging
    // copy or import is needed. The check holds the store lock, exactly as
    // the publisher's own existing-destination check does, so a collection
    // pass whose census predates the record above cannot be removing the
    // tree meanwhile; every later pass sees the record's pin. Anything else
    // — missing or unverifiable — goes through the publisher (which takes
    // the lock itself), and it refuses a corrupt destination.
    let existing = {
        let _guard = SharedStoreLock::acquire(root)?;
        shared_assets::verify_shared_tree(root, &reference).is_ok()
    };
    if !existing {
        let staged_rel = Path::new(&backup).join(STAGING).join(root_key);
        stage_capture(home, &staged_rel, Path::new(root_key), &source, &captured)?;
        shared_assets::import_shared_tree(root, scope, home_path, &staged_rel)?;
        shared_assets::verify_shared_tree(root, &reference)?;
        remove_backup_child(home, &backup, STAGING)?;
    }
    // The link install and original hand-off hold the global lock so store
    // guard scans and collectors never observe a half-switched home.
    let _guard = SharedStoreLock::acquire(root)?;
    home.rename_entry_no_replace(Path::new(root_key), &Path::new(&backup).join(root_key))?;
    let target = shared_assets::shared_tree_root(root, &reference)?;
    home.symlink(&target, Path::new(root_key))?;
    if home.read_link(Path::new(root_key))? != Some(target) {
        return Err(invalid("native cache link did not bind its exact target"));
    }
    discard_freeze_backup(home, &backup, &record("freeze", root_key, &reference))?;
    Ok(FreezeOutcome::Frozen(reference))
}

/// Thaws one frozen native cache root into an independent writable tree.
///
/// The link target is parsed and its store tree fully verified
/// ([`shared_assets::verify_shared_tree`]) before anything is changed. The
/// tree is restored as APFS clones (`clone_tree`: one directory clone, or
/// per-file clones where directories cannot be cloned) — independent inodes
/// with the manifest's logical modes, never an external hardlink — into a
/// staged backup clone, the link is removed, and the real tree is renamed
/// into place under one global `SharedStoreLock` hold. The
/// restored tree contains exactly the captured native entries; the import
/// manifest stays in the store. A root that is already a real directory
/// returns `Ok(None)` — already private — without touching anything. A
/// foreign or dangling link, an unverifiable store tree, or an indexed
/// overlap is an explicit error with the link and store untouched.
pub fn thaw(store_root: &Path, home_path: &Path, root_key: &str) -> Result<Option<SharedTreeRef>> {
    classify(root_key)?;
    let root = validated_store(store_root)?;
    let home_path = home_path
        .canonicalize()
        .map_err(|_| invalid("runtime home must be an existing real directory"))?;
    let home = Dir::open(&home_path)?;
    refuse_index_overlap(&home, root_key)?;
    let reference = match home.entry_type(Path::new(root_key)) {
        Ok(EntryType::Directory) => return Ok(None),
        Ok(EntryType::Symlink) => reference_from_link(&home, root_key, &root)?,
        Ok(kind) => {
            return Err(invalid(format!(
                "native cache root has an unsupported shape: {kind:?}"
            )));
        }
        Err(error) => return Err(error),
    };
    shared_assets::verify_shared_tree(&root, &reference)?;
    let store = Dir::open(&root)?;
    let tree = Path::new("trees")
        .join(&reference.scope)
        .join(&reference.manifest_sha256);
    let backup = format!("{BACKUP_PREFIX}{}", uuid::Uuid::new_v4().simple());
    {
        let _guard = SharedStoreLock::acquire(&root)?;
        write_record(&home, &backup, &record("thaw", root_key, &reference))?;
        clone_tree(&home, &Path::new(&backup).join(CLONE), &store, &tree)?;
        home.remove(Path::new(root_key))?;
        home.rename_entry_no_replace(&Path::new(&backup).join(CLONE), Path::new(root_key))?;
        discard_thaw_backup(&home, &backup)?;
    }
    Ok(Some(reference))
}

/// Clones the verified store tree `tree_rel` (relative to `store`) into
/// `staged` as independent writable real entries with their logical modes —
/// directories `0o700`, files their manifest `0o600`/`0o700`. The store's
/// own snapshot manifest is deliberately not restored: the native root
/// never contained it.
///
/// On a cloning volume the whole hierarchy is cloned in one call, the
/// manifest file is dropped and every manifest entry's mode is set exactly;
/// otherwise each payload is cloned one by one, parents first. Nothing is
/// flushed while staging; before returning, every staged file and directory
/// is pushed with plain `fsync(2)` ([`Dir::push_tree`]) and one [`Dir::sync`]
/// barrier (`F_FULLFSYNC` on Apple hosts, which persists everything pushed
/// before it on the device) makes the whole staged tree durable — on any
/// filesystem, since each object is flushed explicitly. Only then may the
/// caller swap it in; a crash before the swap leaves a staged clone
/// recovery proves against the record or rolls back to the link.
fn clone_tree(home: &Dir, staged: &Path, store: &Dir, tree_rel: &Path) -> Result<()> {
    let tree = store.subdir(tree_rel)?;
    let payload = tree.read(Path::new(SNAPSHOT_MANIFEST), MAX_TREE_MANIFEST_BYTES)?;
    let document: Value = serde_json::from_slice(&payload)
        .map_err(|_| invalid("shared tree manifest is malformed"))?;
    let entries = document
        .get("entries")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("shared tree manifest entries are malformed"))?;
    if entries.len() > MAX_TREE_ENTRIES {
        return Err(invalid("native cache tree exceeds the entry bound"));
    }
    let logical_mode = |entry: &Value| -> Result<u32> {
        if entry["type"] == "directory" {
            return Ok(0o700);
        }
        let mode = entry.get("mode").and_then(Value::as_u64).unwrap_or(0o600) as u32;
        if !matches!(mode, 0o600 | 0o700) {
            return Err(invalid("shared tree manifest mode is not normalized"));
        }
        Ok(mode)
    };
    if home.clone_directory(staged, store, tree_rel)? {
        let clone = home.subdir(staged)?;
        clone.permit_owner_write()?;
        clone.discard(Path::new(SNAPSHOT_MANIFEST))?;
        for entry in entries {
            let relative = Path::new(
                entry["path"]
                    .as_str()
                    .ok_or_else(|| invalid("shared tree manifest entries are malformed"))?,
            );
            clone.set_mode(relative, logical_mode(entry)?)?;
        }
        clone.push_tree()?;
        return clone.sync();
    }
    home.directory(staged)?;
    for entry in entries {
        let relative = Path::new(
            entry["path"]
                .as_str()
                .ok_or_else(|| invalid("shared tree manifest entries are malformed"))?,
        );
        if entry["type"] == "directory" {
            home.stage_directory(&staged.join(relative))?;
            continue;
        }
        let bytes = tree.read(relative, MAX_FILE_BYTES)?;
        let mode = logical_mode(entry)?;
        home.stage_snapshot_file(&staged.join(relative), &tree, relative, &bytes, mode)?;
    }
    let clone = home.subdir(staged)?;
    clone.push_tree()?;
    clone.sync()
}

/// Completes or rolls back every interrupted native-cache operation in one
/// home.
///
/// Each `.agent-run-native-*` backup directory's `op.json` record drives
/// recovery: a freeze whose link is missing is relinked from its verified
/// store tree, a freeze that never moved its original just drops the
/// disposable staging, a thaw whose swap did not land is either completed
/// from a fully proven staged clone or rolled back to the exact link, and
/// finished backups are removed only after their content is proven.
/// Records that are missing or malformed, backups holding unexpected
/// entries, or store trees that no longer verify are explicit errors with
/// every original left in place. History, index bytes, and unrelated home
/// entries are never touched. Homes with no operation backups return
/// `Ok(())` unchanged.
pub fn recover(store_root: &Path, home_path: &Path) -> Result<()> {
    let root = validated_store(store_root)?;
    let home_path = home_path
        .canonicalize()
        .map_err(|_| invalid("runtime home must be an existing real directory"))?;
    let home = Dir::open(&home_path)?;
    let mut backups: Vec<String> = home
        .list(None)?
        .into_iter()
        .filter_map(|name| {
            let text = name.to_str()?;
            text.starts_with(BACKUP_PREFIX).then(|| text.to_owned())
        })
        .collect();
    backups.sort();
    for backup in backups {
        let record = read_record(&home, &backup)?;
        let reference = SharedTreeRef {
            scope: record.scope.clone(),
            manifest_sha256: record.manifest_sha256.clone(),
        };
        let state = home.entry_type(Path::new(&record.root));
        match record.op.as_str() {
            "freeze" => recover_freeze(&home, &root, &backup, &record, &reference, state)?,
            "thaw" => recover_thaw(&home, &root, &backup, &record, &reference, state)?,
            _ => return Err(invalid("native cache operation record is malformed")),
        }
    }
    Ok(())
}

/// Completes or rolls back one interrupted freeze.
fn recover_freeze(
    home: &Dir,
    root: &Path,
    backup: &str,
    record: &OpRecord,
    reference: &SharedTreeRef,
    state: Result<EntryType>,
) -> Result<()> {
    match state {
        Ok(EntryType::Symlink) => {
            if reference_from_link(home, &record.root, root)? != *reference {
                return Err(invalid(format!(
                    "native cache root {} is a foreign link",
                    record.root
                )));
            }
            shared_assets::verify_shared_tree(root, reference)?;
            discard_freeze_backup(home, backup, record)
        }
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            shared_assets::verify_shared_tree(root, reference)?;
            home.symlink(
                &shared_assets::shared_tree_root(root, reference)?,
                Path::new(&record.root),
            )?;
            discard_freeze_backup(home, backup, record)
        }
        Ok(EntryType::Directory) => {
            // The original never moved; the private tree is the valid state
            // and only the disposable staging and record are cleaned.
            remove_backup_child(home, backup, STAGING)?;
            finish_backup_removal(home, backup)
        }
        Ok(kind) => Err(invalid(format!(
            "native cache root {} has an unexpected shape: {kind:?}",
            record.root
        ))),
        Err(error) => Err(error),
    }
}

/// Completes or rolls back one interrupted thaw.
fn recover_thaw(
    home: &Dir,
    root: &Path,
    backup: &str,
    record: &OpRecord,
    reference: &SharedTreeRef,
    state: Result<EntryType>,
) -> Result<()> {
    match state {
        Ok(EntryType::Directory) => discard_thaw_backup(home, backup),
        Ok(EntryType::Symlink) => {
            if reference_from_link(home, &record.root, root)? != *reference {
                return Err(invalid(format!(
                    "native cache root {} is a foreign link",
                    record.root
                )));
            }
            shared_assets::verify_shared_tree(root, reference)?;
            discard_thaw_backup(home, backup)
        }
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            let staged = Path::new(backup).join(CLONE);
            match home.entry_type(&staged) {
                Ok(EntryType::Directory) => {
                    let directory = home.subdir(&staged)?;
                    let capture = capture(&directory, Instant::now() + TIME_BUDGET)?;
                    if capture.manifest_sha256 == record.manifest_sha256 {
                        home.rename_entry_no_replace(&staged, Path::new(&record.root))?;
                        finish_backup_removal(home, backup)
                    } else {
                        rollback_to_link(home, root, backup, record, reference)
                    }
                }
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    rollback_to_link(home, root, backup, record, reference)
                }
                entry => Err(invalid(format!(
                    "thaw backup holds an unexpected shape: {entry:?}"
                ))),
            }
        }
        Ok(kind) => Err(invalid(format!(
            "native cache root {} has an unexpected shape: {kind:?}",
            record.root
        ))),
        Err(error) => Err(error),
    }
}

/// Rolls one interrupted thaw back to its exact verified shared link.
fn rollback_to_link(
    home: &Dir,
    root: &Path,
    backup: &str,
    record: &OpRecord,
    reference: &SharedTreeRef,
) -> Result<()> {
    shared_assets::verify_shared_tree(root, reference)?;
    remove_backup_child(home, backup, CLONE)?;
    home.symlink(
        &shared_assets::shared_tree_root(root, reference)?,
        Path::new(&record.root),
    )?;
    finish_backup_removal(home, backup)
}

/// Scans extant retained homes for native-cache tree references.
///
/// Every supported cache root in each home is checked through no-follow
/// descriptors: a controlled whole-root link contributes its reference, and
/// every `.agent-run-native-*` backup's `op.json` contributes the reference
/// its pending operation holds — so mid-freeze and mid-thaw states are
/// pinned, not lost. Referenced trees' blob sets are then collected through
/// `shared_tree_blob_names`, reading manifests only. The pass is bounded
/// per home by `MAX_CENSUS_PARENTS` and `MAX_CENSUS_BACKUPS`, in
/// aggregate by `MAX_CENSUS_BLOBS` and one `TIME_BUDGET` wall-clock
/// budget, and never by the page size: a larger retained-home set is paged
/// by the caller, resuming at the returned `homes` offset and merging with
/// [`NativeRefScan::merge`]. Any unreadable home, directory, link, record,
/// or manifest sets `complete == false` while keeping the partial
/// references collected so far — a proven-absent directory is simply empty,
/// never an error swallowed as an empty census, and an unrecognized link in
/// a supported cache slot (relative, foreign, or malformed) is unknown
/// reference evidence that marks the pass incomplete while leaving the link
/// untouched. A collector must treat `complete == false` as incomplete
/// evidence and retain, never as an empty reference set.
pub fn scan_refs(store_root: &Path, homes: &[&Path]) -> NativeRefScan {
    let mut scan = NativeRefScan {
        complete: true,
        ..Default::default()
    };
    let Ok(root) = validated_store(store_root) else {
        scan.complete = false;
        return scan;
    };
    let deadline = Instant::now() + TIME_BUDGET;
    for home_path in homes {
        if Instant::now() >= deadline {
            scan.complete = false;
            break;
        }
        scan_home(&root, home_path, &mut scan, deadline);
        // Progress counts a home once its examination finished, whatever it
        // found, so a paged caller resumes exactly after it.
        scan.homes += 1;
    }
    let references: Vec<SharedTreeRef> = scan.trees.values().cloned().collect();
    for reference in &references {
        if scan.blobs.len() > MAX_CENSUS_BLOBS || Instant::now() >= deadline {
            scan.complete = false;
            break;
        }
        match shared_assets::shared_tree_blob_names(&root, reference) {
            Ok(blobs) => scan.blobs.extend(blobs),
            Err(_) => scan.complete = false,
        }
    }
    scan
}

/// Scans one home's cache roots and operation backups into `scan`.
fn scan_home(store_root: &Path, home_path: &Path, scan: &mut NativeRefScan, deadline: Instant) {
    let Ok(home) = Dir::open(home_path) else {
        scan.complete = false;
        return;
    };
    let mut roots = vec![
        "skills/.system".to_owned(),
        CURATED_MIRROR_ROOT.to_owned(),
        CURATED_PACK_ROOT.to_owned(),
    ];
    let mut parents = 0_usize;
    let markets = match home.list(Some(Path::new("plugins/cache"))) {
        Ok(markets) => markets,
        // A home without a native plugin cache is proven empty; every other
        // enumeration failure is partial evidence, never an empty census.
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(_) => {
            scan.complete = false;
            return;
        }
    };
    'outer: for market in markets {
        let Some(market) = market.to_str() else {
            continue;
        };
        if market == "personal" || !safe_component(market) {
            continue;
        }
        let plugins = match home.list(Some(&Path::new("plugins/cache").join(market))) {
            Ok(plugins) => plugins,
            Err(_) => {
                scan.complete = false;
                continue;
            }
        };
        for plugin in plugins {
            let Some(plugin) = plugin.to_str() else {
                continue;
            };
            if !safe_component(plugin) {
                continue;
            }
            parents += 1;
            if parents > MAX_CENSUS_PARENTS || Instant::now() >= deadline {
                scan.complete = false;
                break 'outer;
            }
            roots.push(format!("plugins/cache/{market}/{plugin}"));
        }
    }
    for root_key in roots {
        match home.entry_type(Path::new(&root_key)) {
            Ok(EntryType::Symlink) => match reference_from_link(&home, &root_key, store_root) {
                Ok(reference) => {
                    scan.trees.insert(
                        format!("{}/{}", reference.scope, reference.manifest_sha256),
                        reference,
                    );
                }
                // An unrecognized link in a supported cache slot — relative,
                // foreign, or malformed — is unknown reference evidence: the
                // pass never follows or rewrites it, and reports itself
                // incomplete so a collector retains rather than concluding
                // the slot references nothing.
                Err(_) => scan.complete = false,
            },
            Ok(_) => {}
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => scan.complete = false,
        }
    }
    // The home directory itself already opened, so this listing cannot be
    // proven absent; any failure is partial evidence.
    let Ok(names) = home.list(None) else {
        scan.complete = false;
        return;
    };
    let mut backups = 0_usize;
    for name in names {
        let Some(text) = name.to_str() else {
            continue;
        };
        if !text.starts_with(BACKUP_PREFIX) {
            continue;
        }
        backups += 1;
        if backups > MAX_CENSUS_BACKUPS {
            scan.complete = false;
            break;
        }
        match read_record(&home, text) {
            Ok(found) => {
                scan.backups += 1;
                let key = format!("{}/{}", found.scope, found.manifest_sha256);
                scan.trees.insert(
                    key,
                    SharedTreeRef {
                        scope: found.scope,
                        manifest_sha256: found.manifest_sha256,
                    },
                );
            }
            Err(_) => scan.complete = false,
        }
    }
}
