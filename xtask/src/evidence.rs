//! Verification of the hand-assembled migration evidence inventory.

use crate::release;
use serde_json::Value;
use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

/// Verifies every indexed evidence/ADR file and reports all inventory errors.
///
/// root is the repository root containing migration/evidence/index.json. The
/// index's declared byte counts and SHA-256 values are recomputed from disk;
/// the index itself is excluded from the unlisted-file scan because it cannot
/// contain a stable self-hash. All regular files below migration/evidence and
/// migration/adr must otherwise be listed. The returned count is the number
/// of index entries examined on success.
pub fn verify(root: &Path) -> Result<usize, String> {
    let index_path = root.join("migration/evidence/index.json");
    let document: Value =
        serde_json::from_slice(&fs::read(&index_path).map_err(|error| error.to_string())?)
            .map_err(|error| format!("invalid evidence index: {error}"))?;
    if document["index_version"].as_u64() != Some(1) {
        return Err("unsupported evidence index version".into());
    }
    let entries = document["entries"]
        .as_array()
        .ok_or("evidence index has no entries")?;
    let declared_count = document["entry_count"].as_u64();
    let mut errors = Vec::new();
    if declared_count != Some(entries.len() as u64) {
        errors.push(format!(
            "entry count mismatch: expected {}, got {}",
            declared_count.map_or_else(|| "missing".into(), |count| count.to_string()),
            entries.len()
        ));
    }
    let mut listed = BTreeSet::new();
    for entry in entries {
        verify_entry(root, entry, &mut listed, &mut errors);
    }
    for directory in ["migration/evidence", "migration/adr"] {
        let directory = root.join(directory);
        match files(&directory) {
            Ok(files) => {
                for path in files {
                    let relative = path.strip_prefix(root).expect("root descendant");
                    let name = relative.to_string_lossy().into_owned();
                    if name != "migration/evidence/index.json" && !listed.contains(&name) {
                        errors.push(format!("unlisted evidence file: {name}"));
                    }
                }
            }
            Err(_) => errors.push(format!(
                "missing evidence directory: {}",
                directory.display()
            )),
        }
    }
    if errors.is_empty() {
        Ok(entries.len())
    } else {
        Err(errors.join("\n"))
    }
}

/// Checks one inventory entry and records every failure it exposes.
fn verify_entry(
    root: &Path,
    entry: &Value,
    listed: &mut BTreeSet<String>,
    errors: &mut Vec<String>,
) {
    let Some(raw_path) = entry["path"].as_str() else {
        errors.push("evidence entry has no path".into());
        return;
    };
    let Ok(path) = relative_path(raw_path) else {
        errors.push(format!("unsafe evidence path: {raw_path}"));
        return;
    };
    if !listed.insert(raw_path.to_owned()) {
        errors.push(format!("duplicate evidence entry: {raw_path}"));
        return;
    }
    let full = root.join(&path);
    let metadata = match fs::metadata(&full) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => {
            errors.push(format!("evidence path is not a regular file: {raw_path}"));
            return;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            errors.push(format!("missing evidence file: {raw_path}"));
            return;
        }
        Err(error) => {
            errors.push(format!("cannot read evidence file {raw_path}: {error}"));
            return;
        }
    };
    let expected_bytes = entry["bytes"].as_u64();
    if expected_bytes != Some(metadata.len()) {
        errors.push(format!(
            "size mismatch: {raw_path} (expected {}, got {})",
            expected_bytes.map_or_else(|| "missing".into(), |bytes| bytes.to_string()),
            metadata.len()
        ));
    }
    let expected_hash = entry["sha256"].as_str();
    match fs::read(&full) {
        Ok(bytes) => {
            let actual_hash = release::digest_bytes(&bytes);
            if expected_hash != Some(actual_hash.as_str()) {
                errors.push(format!(
                    "hash mismatch: {raw_path} (expected {}, got {actual_hash})",
                    expected_hash.unwrap_or("missing")
                ));
            }
        }
        Err(error) => errors.push(format!("cannot hash evidence file {raw_path}: {error}")),
    }
}

/// Returns all regular files below a directory in deterministic order.
fn files(directory: &Path) -> Result<Vec<PathBuf>, String> {
    let mut result = Vec::new();
    walk_files(directory, &mut result)?;
    result.sort();
    Ok(result)
}

/// Recurses through one evidence directory without following directories via links.
fn walk_files(directory: &Path, result: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in fs::read_dir(directory).map_err(|error| error.to_string())? {
        let path = entry.map_err(|error| error.to_string())?.path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        if metadata.is_dir() {
            walk_files(&path, result)?;
        } else if metadata.is_file() {
            result.push(path);
        }
    }
    Ok(())
}

/// Rejects absolute and parent-traversing inventory paths.
fn relative_path(path: &str) -> Result<PathBuf, String> {
    let path = Path::new(path);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        Err("unsafe relative path".into())
    } else {
        Ok(path.to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::verify;
    use sha2::{Digest, Sha256};
    use std::fs;
    use tempfile::tempdir;

    /// Hashes fixture bytes in the same lowercase form as the verifier.
    fn hash(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    /// Confirms the verifier reports both a changed file and an unlisted file.
    #[test]
    fn reports_mismatches_and_unlisted_files() {
        // Rust-only tooling has no Python counterpart and therefore no Mirrors citation.
        let temporary = tempdir().expect("temporary root");
        let evidence = temporary.path().join("migration/evidence");
        let adr = temporary.path().join("migration/adr");
        fs::create_dir_all(&adr).expect("ADR directory");
        fs::create_dir_all(&evidence).expect("evidence directory");
        fs::write(evidence.join("one.md"), "changed").expect("evidence file");
        fs::write(adr.join("A1.md"), "unlisted").expect("ADR file");
        let index = serde_json::json!({
            "index_version": 1,
            "entry_count": 1,
            "entries": [{
                "path": "migration/evidence/one.md",
                "bytes": 7,
                "sha256": hash(b"original")
            }]
        });
        fs::write(
            evidence.join("index.json"),
            serde_json::to_vec(&index).expect("index"),
        )
        .expect("index file");
        let error = verify(temporary.path()).expect_err("tampered inventory");
        assert!(error.contains("hash mismatch: migration/evidence/one.md"));
        assert!(error.contains("unlisted evidence file: migration/adr/A1.md"));
    }
}
