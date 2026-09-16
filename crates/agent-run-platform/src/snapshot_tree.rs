//! Python-v1 immutable managed-tree manifests and runtime snapshot indexes.
//!
//! This module owns the portable on-disk contract shared with
//! `adapters/snapshot_tree.py` and `adapters/snapshot_runtime.py`: files are
//! published before a canonical manifest, manifests are bound by a canonical
//! runtime index, and inspection reports drift without repairing it.

use crate::{
    fs::{self, Dir, EntryType},
    publish::{self, Entry},
};
use agent_run_domain::{canonical, error::invalid, Error, Result};
use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

/// Per-tree Python-v1 manifest filename.
pub const SNAPSHOT_MANIFEST: &str = ".agent-run-snapshot.json";
/// Generated-home Python-v1 runtime index filename.
pub const RUNTIME_SNAPSHOT_INDEX: &str = ".agent-run-snapshots.json";
const MAX_METADATA: usize = 64 * 1024;
const TEMP_PREFIX: &str = ".agent-run-";
/// Canonically sorted entries and their exact regular-file bytes.
type TreeRead = (Vec<Value>, BTreeMap<String, Vec<u8>>);

/// Identity returned after publishing a tree and its manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeSnapshot {
    /// SHA-256 of the exact canonical manifest bytes.
    pub sha256: String,
    /// Manifest path below the generated home.
    pub manifest_path: PathBuf,
    /// Canonically sorted paths represented by the manifest.
    pub entries: Vec<String>,
}

/// Non-destructive classification of one tree's recovery state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotInspection {
    /// True only when metadata and the complete topology/content agree.
    pub verified: bool,
    /// Publisher-owned temporary entries left by an interrupted write.
    pub owned_temps: Vec<String>,
    /// Entries not represented by the manifest.
    pub orphans: Vec<String>,
    /// Manifest entries absent from the actual tree.
    pub referenced_missing: Vec<String>,
    /// Aggregate type and hash mismatch paths.
    pub mismatched: Vec<String>,
    /// Entries whose file kind differs from the manifest.
    pub type_mismatches: Vec<String>,
    /// Same-kind entries whose metadata/content differs.
    pub hash_mismatches: Vec<String>,
}

/// Aggregate no-repair verification result for the complete runtime home.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeSnapshotInspection {
    /// True only when the index, all trees, files, and links match exactly.
    pub verified: bool,
    /// Referenced files or manifests that are absent.
    pub missing: Vec<String>,
    /// Aggregate type and hash mismatch paths.
    pub mismatched: Vec<String>,
    /// Publisher-owned temporaries in an indexed tree.
    pub owned_temps: Vec<String>,
    /// Unexpected entries in an indexed tree.
    pub orphans: Vec<String>,
    /// Wrong-kind paths, including symlinks.
    pub type_mismatches: Vec<String>,
    /// Same-kind paths with changed content or targets.
    pub hash_mismatches: Vec<String>,
}

fn canonical(value: &Value) -> Vec<u8> {
    let mut bytes = canonical::dumps(value, true);
    bytes.push(b'\n');
    bytes
}

fn relative(path: &Path, label: &str) -> Result<()> {
    fs::relative(path)
        .map(drop)
        .map_err(|_| invalid(format!("{label} must be a relative path without '..'")))
}

fn portable(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid("snapshot paths must be UTF-8"))
}

fn entry(path: String, kind: EntryType, bytes: Option<Vec<u8>>) -> Result<Value> {
    Ok(match kind {
        EntryType::Directory => json!({"path": path, "type": "directory"}),
        EntryType::File => {
            let bytes = bytes.expect("file entries always include bytes");
            json!({"path": path, "type": "file", "mode": 0o600, "bytes": bytes.len(), "sha256": fs::sha256(&bytes)})
        }
        EntryType::Symlink | EntryType::Special => {
            return Err(invalid(format!("snapshot entry must be regular: {path}")))
        }
    })
}

fn read_tree(source: &Path, selected: Option<&[String]>, allow_special: bool) -> Result<TreeRead> {
    if !source.is_absolute() {
        return Err(invalid("snapshot source must be an absolute directory"));
    }
    let directory =
        Dir::open(source).map_err(|_| invalid("snapshot source must be a real directory"))?;
    let selected: Option<Vec<PathBuf>> = selected
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    let path = PathBuf::from(item);
                    relative(&path, "snapshot asset")?;
                    Ok::<PathBuf, Error>(path)
                })
                .collect()
        })
        .transpose()?;
    let mut entries = Vec::new();
    let mut files = BTreeMap::new();
    fn walk(
        directory: &Dir,
        prefix: &Path,
        selected: Option<&[PathBuf]>,
        allow_special: bool,
        entries: &mut Vec<Value>,
        files: &mut BTreeMap<String, Vec<u8>>,
    ) -> Result<()> {
        for name in directory.list(if prefix.as_os_str().is_empty() {
            None
        } else {
            Some(prefix)
        })? {
            let relative_path = prefix.join(name);
            let included = selected
                .map(|items| {
                    items.iter().any(|item| {
                        relative_path.starts_with(item) || item.starts_with(&relative_path)
                    })
                })
                .unwrap_or(true);
            if !included {
                continue;
            }
            let path = portable(&relative_path)?;
            match directory.entry_type(&relative_path)? {
                EntryType::Directory => {
                    entries.push(entry(path, EntryType::Directory, None)?);
                    walk(
                        directory,
                        &relative_path,
                        selected,
                        allow_special,
                        entries,
                        files,
                    )?;
                }
                EntryType::File => {
                    let payload = directory.read(&relative_path, 16 * 1024 * 1024)?;
                    let mut value = entry(path.clone(), EntryType::File, Some(payload.clone()))?;
                    let executable = directory
                        .open_file(&relative_path)?
                        .metadata()?
                        .permissions()
                        .mode()
                        & 0o111
                        != 0;
                    value["mode"] = json!(if executable { 0o700 } else { 0o600 });
                    files.insert(path, payload);
                    entries.push(value);
                }
                EntryType::Symlink | EntryType::Special if allow_special => {
                    entries.push(json!({"path": path, "type": if directory.entry_type(&relative_path)? == EntryType::Symlink { "symlink" } else { "special" }}));
                }
                EntryType::Symlink | EntryType::Special => {
                    return Err(invalid(format!("snapshot entry must be regular: {path}")))
                }
            }
        }
        Ok(())
    }
    use std::os::unix::fs::PermissionsExt;
    walk(
        &directory,
        Path::new(""),
        selected.as_deref(),
        allow_special,
        &mut entries,
        &mut files,
    )?;
    entries.sort_by_key(|value| value["path"].as_str().unwrap_or_default().to_owned());
    if let Some(selected) = selected {
        let found: BTreeSet<_> = entries
            .iter()
            .filter_map(|value| value["path"].as_str())
            .collect();
        let missing: Vec<_> = selected
            .iter()
            .filter(|item| !found.contains(item.to_string_lossy().as_ref()))
            .map(|item| item.to_string_lossy().into_owned())
            .collect();
        if !missing.is_empty() {
            return Err(invalid(format!(
                "snapshot assets are missing: {}",
                missing.join(", ")
            )));
        }
    }
    Ok((entries, files))
}

fn manifest(entries: &[Value]) -> Vec<u8> {
    canonical(&json!({"snapshot_version": 1, "entries": entries}))
}

fn load_manifest(dir: &Dir) -> Result<Option<Vec<Value>>> {
    let Some(raw) = dir.optional(Path::new(SNAPSHOT_MANIFEST), MAX_METADATA)? else {
        return Ok(None);
    };
    let document: Value =
        serde_json::from_slice(&raw).map_err(|_| invalid("snapshot manifest is malformed"))?;
    let entries = document
        .get("entries")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| invalid("snapshot manifest entries are malformed"))?;
    if document.get("snapshot_version") != Some(&json!(1))
        || entries.iter().any(|entry| !entry.is_object())
    {
        return Err(invalid("snapshot manifest version is unsupported"));
    }
    Ok(Some(entries))
}

fn entry_map(entries: &[Value]) -> Result<BTreeMap<String, Value>> {
    let mut mapped = BTreeMap::new();
    for entry in entries {
        let path = entry
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("snapshot manifest entries are malformed"))?;
        let kind = entry
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("snapshot manifest entries are malformed"))?;
        if !matches!(kind, "directory" | "file") || path == SNAPSHOT_MANIFEST {
            return Err(invalid("snapshot manifest path is invalid"));
        }
        relative(Path::new(path), "snapshot manifest path")?;
        if mapped.insert(path.into(), entry.clone()).is_some() {
            return Err(invalid("snapshot manifest path is duplicated"));
        }
    }
    Ok(mapped)
}

fn register_snapshot(home: &Path, root: &Path) -> Result<()> {
    let directory = Dir::open(home)?;
    let mut roots = match directory.optional(Path::new(RUNTIME_SNAPSHOT_INDEX), MAX_METADATA)? {
        None => Vec::new(),
        Some(raw) => serde_json::from_slice::<Value>(&raw)
            .ok()
            .and_then(|document| document.get("roots").and_then(Value::as_array).cloned())
            .ok_or_else(|| invalid("runtime snapshot index is malformed"))?
            .into_iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| invalid("runtime snapshot index is malformed"))
            })
            .collect::<Result<Vec<_>>>()?,
    };
    let root = portable(root)?;
    if !roots.contains(&root) {
        roots.push(root);
    }
    roots.sort();
    roots.dedup();
    let bytes = canonical(&json!({"snapshot_index_version": 1, "roots": roots}));
    directory.write(Path::new(RUNTIME_SNAPSHOT_INDEX), &bytes, 0o600)
}

/// Publish a complete source tree and its Python-v1 manifest last.
pub fn snapshot_managed_tree(
    home: &Path,
    relative_root: &Path,
    source: &Path,
    selected: Option<&[String]>,
) -> Result<TreeSnapshot> {
    relative(relative_root, "snapshot destination")?;
    fs::private_dir(home)?;
    let (entries, files) = read_tree(source, selected, false)?;
    if entries
        .iter()
        .any(|entry| entry["path"] == SNAPSHOT_MANIFEST)
    {
        return Err(invalid(
            "snapshot source uses reserved name: .agent-run-snapshot.json",
        ));
    }
    let home_dir = Dir::open(home)?;
    match home_dir.entry_type(relative_root) {
        Ok(EntryType::Directory) => {
            let existing = inspect_managed_snapshot(home, relative_root)?;
            let root_dir = Dir::open(&home.join(relative_root))?;
            if !root_dir.list(None)?.is_empty() && !existing.verified {
                return Err(invalid("existing managed snapshot requires recovery"));
            }
            if existing.verified {
                let old = entry_map(&load_manifest(&root_dir)?.expect("verified has manifest"))?;
                let new = entry_map(&entries)?;
                if old.keys().ne(new.keys())
                    || old
                        .iter()
                        .any(|(path, old)| old["type"] != new[path]["type"])
                {
                    return Err(invalid(
                        "snapshot publication cannot change existing topology",
                    ));
                }
            }
        }
        Ok(_) => return Err(invalid("snapshot destination must be a real directory")),
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            home_dir.directory(relative_root)?
        }
        Err(error) => return Err(error),
    }
    for entry in &entries {
        if entry["type"] == "directory" {
            home_dir
                .directory(&relative_root.join(entry["path"].as_str().expect("paths validated")))?;
        }
    }
    let map = entry_map(&entries)?;
    for (path, payload) in &files {
        let mode = map[path]["mode"].as_u64().expect("file mode") as u32;
        home_dir.write(&relative_root.join(path), payload, mode)?;
    }
    let document = manifest(&entries);
    publish::publish_group(
        &home_dir,
        &[Entry::new(
            &relative_root.join(SNAPSHOT_MANIFEST),
            &document,
            0o600,
        )],
    )?;
    register_snapshot(home, relative_root)?;
    Ok(TreeSnapshot {
        sha256: fs::sha256(&document),
        manifest_path: relative_root.join(SNAPSHOT_MANIFEST),
        entries: entries
            .iter()
            .filter_map(|entry| entry["path"].as_str().map(str::to_owned))
            .collect(),
    })
}

/// Copy only explicitly selected files or directories into a managed snapshot.
///
/// Selection is relative to `source`, preserves the selected layout below
/// `relative_root`, rejects links and missing entries, and leaves unrelated
/// source files out of the generated home.
pub fn snapshot_selected_assets(
    home: &Path,
    relative_root: &Path,
    source: &Path,
    selected: &[String],
) -> Result<TreeSnapshot> {
    snapshot_managed_tree(home, relative_root, source, Some(selected))
}

/// Inspect one published tree without deleting, rewriting, or following links.
pub fn inspect_managed_snapshot(home: &Path, relative_root: &Path) -> Result<SnapshotInspection> {
    relative(relative_root, "snapshot destination")?;
    let home_dir = Dir::open(home)?;
    match home_dir.entry_type(relative_root) {
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(SnapshotInspection {
                referenced_missing: vec![SNAPSHOT_MANIFEST.into()],
                ..Default::default()
            })
        }
        Ok(EntryType::Directory) => {}
        Ok(_) => return Err(invalid("snapshot destination must be a real directory")),
        Err(error) => return Err(error),
    }
    let root = home.join(relative_root);
    let root_dir = Dir::open(&root)?;
    let expected = load_manifest(&root_dir)?;
    let (actual_entries, _) = read_tree(&root, None, true)?;
    let mut actual = BTreeMap::new();
    let mut owned_temps = Vec::new();
    for entry in actual_entries {
        let path = entry["path"].as_str().expect("tree path").to_owned();
        if path == SNAPSHOT_MANIFEST {
            continue;
        }
        if path
            .rsplit('/')
            .next()
            .is_some_and(|name| name.starts_with(TEMP_PREFIX) && name.ends_with(".tmp"))
        {
            owned_temps.push(path);
        } else {
            actual.insert(path, entry);
        }
    }
    let Some(expected) = expected else {
        return Ok(SnapshotInspection {
            verified: false,
            owned_temps,
            orphans: actual.into_keys().collect(),
            referenced_missing: vec![SNAPSHOT_MANIFEST.into()],
            ..Default::default()
        });
    };
    let expected = entry_map(&expected)?;
    let missing: Vec<_> = expected
        .keys()
        .filter(|path| !actual.contains_key(*path))
        .cloned()
        .collect();
    let orphans: Vec<_> = actual
        .keys()
        .filter(|path| !expected.contains_key(*path))
        .cloned()
        .collect();
    let mut type_mismatches = Vec::new();
    let mut hash_mismatches = Vec::new();
    for path in expected.keys().filter(|path| actual.contains_key(*path)) {
        if expected[path]["type"] != actual[path]["type"] {
            type_mismatches.push(path.clone());
        } else if expected[path] != actual[path] {
            hash_mismatches.push(path.clone());
        }
    }
    let mut mismatched = type_mismatches.clone();
    mismatched.extend(hash_mismatches.clone());
    Ok(SnapshotInspection {
        verified: owned_temps.is_empty()
            && missing.is_empty()
            && orphans.is_empty()
            && mismatched.is_empty(),
        owned_temps,
        orphans,
        referenced_missing: missing,
        mismatched,
        type_mismatches,
        hash_mismatches,
    })
}

/// Finalize the Python-v1 runtime index and return its exact SHA-256.
pub fn finalize_runtime_snapshots(
    home: &Path,
    materialize_revision: &str,
    managed_files: &[String],
    managed_links: &[(String, String)],
) -> Result<String> {
    if materialize_revision.trim().is_empty() {
        return Err(invalid("materialize revision must be nonblank"));
    }
    let directory = Dir::open(home)?;
    let roots: Vec<String> =
        match directory.optional(Path::new(RUNTIME_SNAPSHOT_INDEX), MAX_METADATA)? {
            None => Vec::new(),
            Some(raw) => serde_json::from_slice::<Value>(&raw)
                .ok()
                .and_then(|document| document.get("roots").and_then(Value::as_array).cloned())
                .ok_or_else(|| invalid("runtime snapshot index is malformed"))?
                .into_iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| invalid("runtime snapshot index is malformed"))
                })
                .collect::<Result<Vec<_>>>()?,
        };
    if roots.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid("runtime snapshot index roots are malformed"));
    }
    let mut manifests = Map::new();
    for root in &roots {
        let inspection = inspect_managed_snapshot(home, Path::new(root))?;
        if !inspection.verified {
            return Err(invalid(format!("managed snapshot is not verified: {root}")));
        }
        manifests.insert(
            root.clone(),
            json!(fs::sha256(
                &Dir::open(&home.join(root))?.read(Path::new(SNAPSHOT_MANIFEST), MAX_METADATA)?
            )),
        );
    }
    let (_, payloads) = read_tree(home, Some(managed_files), false)?;
    let mut files = Vec::new();
    for path in managed_files {
        let payload = payloads
            .get(path)
            .ok_or_else(|| invalid("managed runtime file is missing"))?;
        let executable = directory
            .open_file(Path::new(path))?
            .metadata()?
            .permissions()
            .mode()
            & 0o111
            != 0;
        files.push(json!({"path": path, "type": "file", "mode": if executable { 0o700 } else { 0o600 }, "bytes": payload.len(), "sha256": fs::sha256(payload)}));
    }
    use std::os::unix::fs::PermissionsExt;
    files.sort_by_key(|entry| entry["path"].as_str().unwrap_or_default().to_owned());
    let mut links = Vec::new();
    for (path, target) in managed_links {
        relative(Path::new(path), "managed runtime link")?;
        if target.is_empty()
            || directory.read_link(Path::new(path))?.as_deref() != Some(Path::new(target))
        {
            return Err(invalid(format!(
                "managed runtime link target does not match: {path}"
            )));
        }
        links.push(json!({"path": path, "target": target}));
    }
    links.sort_by_key(|entry| entry["path"].as_str().unwrap_or_default().to_owned());
    let bytes = canonical(
        &json!({"snapshot_index_version": 1, "materialize_revision": materialize_revision, "roots": roots, "manifests": manifests, "files": files, "links": links}),
    );
    if bytes.len() > MAX_METADATA {
        return Err(invalid("runtime snapshot index exceeds the metadata bound"));
    }
    directory.write(Path::new(RUNTIME_SNAPSHOT_INDEX), &bytes, 0o600)?;
    Ok(fs::sha256(&bytes))
}

/// Verify a v1 index against its recorded revision and digest without repair.
pub fn inspect_runtime_snapshots(
    home: &Path,
    expected_revision: &str,
    expected_sha256: &str,
) -> Result<RuntimeSnapshotInspection> {
    if expected_revision.trim().is_empty() || !is_sha256(expected_sha256) {
        return Err(invalid("runtime snapshot index expectation is invalid"));
    }
    let directory = Dir::open(home)?;
    let raw = directory
        .optional(Path::new(RUNTIME_SNAPSHOT_INDEX), MAX_METADATA)?
        .ok_or_else(|| invalid("runtime snapshot index is missing"))?;
    if fs::sha256(&raw) != expected_sha256 {
        return Err(invalid(
            "runtime snapshot index hash does not match config snapshot",
        ));
    }
    let document: Value =
        serde_json::from_slice(&raw).map_err(|_| invalid("runtime snapshot index is malformed"))?;
    let required: BTreeSet<_> = [
        "snapshot_index_version",
        "materialize_revision",
        "roots",
        "manifests",
        "files",
        "links",
    ]
    .into_iter()
    .collect();
    if document
        .as_object()
        .map(|value| value.keys().map(String::as_str).collect())
        != Some(required)
        || document.get("snapshot_index_version") != Some(&json!(1))
        || raw != canonical(&document)
    {
        return Err(invalid("runtime snapshot index is malformed"));
    }
    if document["materialize_revision"] != expected_revision {
        return Err(invalid(
            "runtime snapshot index revision does not match config snapshot",
        ));
    }
    let roots = document["roots"]
        .as_array()
        .ok_or_else(|| invalid("runtime snapshot index roots are malformed"))?;
    let roots: Vec<_> = roots
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid("runtime snapshot index roots are malformed"))
        })
        .collect::<Result<_>>()?;
    if roots.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid("runtime snapshot index roots are malformed"));
    }
    let manifests = document["manifests"]
        .as_object()
        .ok_or_else(|| invalid("runtime snapshot index manifests are malformed"))?;
    if manifests.len() != roots.len()
        || roots.iter().any(|root| {
            !manifests
                .get(root)
                .is_some_and(|value| value.as_str().is_some_and(is_sha256))
        })
    {
        return Err(invalid("runtime snapshot index manifests are malformed"));
    }
    let mut result = RuntimeSnapshotInspection::default();
    for root in roots {
        let manifest_path = format!("{root}/{SNAPSHOT_MANIFEST}");
        match Dir::open(&home.join(&root))
            .and_then(|dir| dir.read(Path::new(SNAPSHOT_MANIFEST), MAX_METADATA))
        {
            Ok(bytes)
                if fs::sha256(&bytes) != manifests[&root].as_str().expect("validated digest") =>
            {
                result.hash_mismatches.push(manifest_path)
            }
            Ok(_) => {}
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                result.missing.push(manifest_path)
            }
            Err(_) => result.type_mismatches.push(manifest_path),
        }
        match inspect_managed_snapshot(home, Path::new(&root)) {
            Ok(tree) => {
                result.missing.extend(
                    tree.referenced_missing
                        .into_iter()
                        .map(|path| format!("{root}/{path}")),
                );
                result.type_mismatches.extend(
                    tree.type_mismatches
                        .into_iter()
                        .map(|path| format!("{root}/{path}")),
                );
                result.hash_mismatches.extend(
                    tree.hash_mismatches
                        .into_iter()
                        .map(|path| format!("{root}/{path}")),
                );
                result.owned_temps.extend(
                    tree.owned_temps
                        .into_iter()
                        .map(|path| format!("{root}/{path}")),
                );
                result.orphans.extend(
                    tree.orphans
                        .into_iter()
                        .map(|path| format!("{root}/{path}")),
                );
            }
            Err(_) => result.type_mismatches.push(root),
        }
    }
    let files = document["files"]
        .as_array()
        .ok_or_else(|| invalid("runtime snapshot index files are malformed"))?;
    for expected in files {
        let path = expected
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("runtime snapshot index files are malformed"))?;
        match read_tree(home, Some(&[path.into()]), true) {
            Ok((entries, _)) => match entries.iter().find(|entry| entry["path"] == path) {
                Some(actual) if actual == expected => {}
                Some(actual) if actual["type"] == expected["type"] => {
                    result.hash_mismatches.push(path.into())
                }
                Some(_) => result.type_mismatches.push(path.into()),
                None => result.missing.push(path.into()),
            },
            Err(_) => match directory.entry_type(Path::new(path)) {
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    result.missing.push(path.into())
                }
                Ok(EntryType::File) => result.hash_mismatches.push(path.into()),
                _ => result.type_mismatches.push(path.into()),
            },
        }
    }
    let links = document["links"]
        .as_array()
        .ok_or_else(|| invalid("runtime snapshot index links are malformed"))?;
    for expected in links {
        let path = expected
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("runtime snapshot index links are malformed"))?;
        let target = expected
            .get("target")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("runtime snapshot index links are malformed"))?;
        match directory.read_link(Path::new(path)) {
            Ok(None) => result.missing.push(path.into()),
            Ok(Some(actual)) if actual == Path::new(target) => {}
            Ok(Some(_)) => result.hash_mismatches.push(path.into()),
            Err(_) => result.type_mismatches.push(path.into()),
        }
    }
    result.missing.sort();
    result.missing.dedup();
    result.type_mismatches.sort();
    result.type_mismatches.dedup();
    result.hash_mismatches.sort();
    result.hash_mismatches.dedup();
    result.owned_temps.sort();
    result.owned_temps.dedup();
    result.orphans.sort();
    result.orphans.dedup();
    result.mismatched = result.type_mismatches.clone();
    result.mismatched.extend(result.hash_mismatches.clone());
    result.mismatched.sort();
    result.mismatched.dedup();
    result.verified = result.missing.is_empty()
        && result.mismatched.is_empty()
        && result.owned_temps.is_empty()
        && result.orphans.is_empty();
    Ok(result)
}

/// Return the hash of a complete, verified runtime snapshot index.
pub fn runtime_snapshot_index_sha256(home: &Path, expected_revision: &str) -> Result<String> {
    let directory = Dir::open(home)?;
    let raw = directory
        .optional(Path::new(RUNTIME_SNAPSHOT_INDEX), MAX_METADATA)?
        .ok_or_else(|| invalid("runtime snapshot index is missing"))?;
    let digest = fs::sha256(&raw);
    let inspection = inspect_runtime_snapshots(home, expected_revision, &digest)?;
    if !inspection.verified {
        return Err(invalid(
            "runtime snapshot index references unverified artifacts",
        ));
    }
    Ok(digest)
}

/// Return whether a string is a Python-compatible SHA-256 hexadecimal value.
pub fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
