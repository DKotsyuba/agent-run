//! Reproducible source archive creation and content verification.

use crate::release;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    env, fs,
    path::{Component, Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

/// Filename of the manifest stored at the archive prefix.
const MANIFEST: &str = "ARCHIVE-MANIFEST.json";

/// One file or symbolic link recorded in an archive manifest.
struct FileRecord {
    /// The path relative to the archive's top-level directory.
    path: PathBuf,
    /// The number of bytes represented by the file or link target.
    bytes: u64,
    /// The lowercase SHA-256 digest of the represented bytes.
    sha256: String,
}

/// Creates a source archive for revision and writes its manifest into it.
///
/// root is the Git worktree, output is replaced if it exists, and revision is
/// any commit-ish accepted by Git. The archive contains only tracked files
/// from that revision; build output and per-worktree caches are excluded even
/// if a revision happens to track such a path. The manifest records the
/// resolved commit, every included file, and the Cargo.lock digest. Errors
/// include Git, tar, filesystem, or malformed-tree failures.
pub fn build(root: &Path, output: &Path, revision: &str) -> Result<PathBuf, String> {
    let commit = resolve_commit(root, revision)?;
    let short = &commit[..commit.len().min(12)];
    let prefix = format!("agent-run-source-{short}");
    let temporary = temporary_directory("archive")?;
    let result = (|| {
        let preliminary = temporary.join("source.tar");
        git_archive(root, &commit, &preliminary, &prefix, None)?;
        let staged = temporary.join("staged");
        extract(&preliminary, &staged)?;
        let records = records(&staged.join(&prefix))?;
        if records
            .iter()
            .any(|record| record.path == Path::new(MANIFEST))
        {
            return Err(format!("revision already contains {MANIFEST}"));
        }
        let lock = records
            .iter()
            .find(|record| record.path == Path::new("Cargo.lock"))
            .ok_or("archive revision has no Cargo.lock")?;
        let manifest = json!({
            "format": 1,
            "commit": commit,
            "lock": {
                "path": "Cargo.lock",
                "bytes": lock.bytes,
                "sha256": lock.sha256,
            },
            "files": records.iter().map(|record| json!({
                "path": record.path.to_string_lossy(),
                "bytes": record.bytes,
                "sha256": record.sha256,
            })).collect::<Vec<_>>(),
        });
        let manifest =
            serde_json::to_string_pretty(&manifest).map_err(|error| error.to_string())? + "\n";
        git_archive(root, &commit, output, &prefix, Some(&manifest))?;
        Ok(output.to_path_buf())
    })();
    let _ = fs::remove_dir_all(&temporary);
    result
}

/// Verifies an archive's manifest, commit reference, lock entry, and contents.
///
/// The archive is extracted into a private temporary directory. Every
/// manifest digest and byte count is recomputed from the extracted content,
/// and every regular file or symbolic link in the archive must be listed.
/// Verification returns all detected content errors together so one run does
/// not hide later mismatches. The archive itself is never modified.
pub fn verify(root: &Path, archive: &Path) -> Result<(), String> {
    if !archive.is_file() {
        return Err(format!("archive is not a file: {}", archive.display()));
    }
    let temporary = temporary_directory("archive-verify")?;
    let result = (|| {
        extract(archive, &temporary)?;
        let manifests = records(&temporary)?
            .into_iter()
            .filter(|record| {
                record.path.file_name().and_then(|name| name.to_str()) == Some(MANIFEST)
            })
            .collect::<Vec<_>>();
        if manifests.len() != 1 {
            return Err(format!("archive must contain exactly one {MANIFEST}"));
        }
        let manifest_path = temporary.join(&manifests[0].path);
        let manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).map_err(|error| error.to_string())?)
                .map_err(|error| format!("invalid archive manifest: {error}"))?;
        let commit = manifest["commit"]
            .as_str()
            .ok_or("archive manifest has no commit")?;
        validate_commit(root, commit)?;
        let listed = manifest_records(&manifest)?;
        let archive_root = manifest_path
            .parent()
            .ok_or("archive manifest has no parent")?;
        let actual = records(archive_root)?
            .into_iter()
            .filter(|record| record.path != Path::new(MANIFEST))
            .map(|record| (record.path.to_string_lossy().into_owned(), record))
            .collect::<BTreeMap<_, _>>();
        let mut errors = Vec::new();
        for (path, expected) in &listed {
            match actual.get(path) {
                None => errors.push(format!("missing archive file: {path}")),
                Some(found) => {
                    if found.bytes != expected.bytes {
                        errors.push(format!(
                            "size mismatch: {path} (expected {}, got {})",
                            expected.bytes, found.bytes
                        ));
                    }
                    if found.sha256 != expected.sha256 {
                        errors.push(format!(
                            "hash mismatch: {path} (expected {}, got {})",
                            expected.sha256, found.sha256
                        ));
                    }
                }
            }
        }
        for path in actual.keys() {
            if !listed.contains_key(path) {
                errors.push(format!("unlisted archive file: {path}"));
            }
        }
        verify_lock(&manifest, &listed, &mut errors);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("\n"))
        }
    })();
    let _ = fs::remove_dir_all(&temporary);
    result
}

/// Returns the default archive destination for a revision.
pub fn default_output(root: &Path, revision: &str) -> Result<PathBuf, String> {
    let commit = resolve_commit(root, revision)?;
    Ok(root.join("dist").join(format!(
        "agent-run-source-{}.tar",
        &commit[..commit.len().min(12)]
    )))
}

/// Resolves a commit-ish to its complete Git object name.
fn resolve_commit(root: &Path, revision: &str) -> Result<String, String> {
    if revision.is_empty() || revision.starts_with('-') || revision.chars().any(char::is_whitespace)
    {
        return Err("revision must be a nonblank Git revision without whitespace".into());
    }
    let object = format!("{revision}^{{commit}}");
    let output = Command::new("git")
        .current_dir(root)
        .args(["rev-parse", "--verify", "--end-of-options", &object])
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!("cannot resolve revision {revision}"));
    }
    let commit = String::from_utf8(output.stdout)
        .map_err(|error| error.to_string())?
        .trim()
        .to_owned();
    if commit.is_empty() || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("Git returned an invalid commit id".into());
    }
    Ok(commit)
}

/// Confirms that a manifest commit is present in the supplied Git worktree.
fn validate_commit(root: &Path, commit: &str) -> Result<(), String> {
    if (commit.len() != 40 && commit.len() != 64)
        || !commit.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("archive manifest has an invalid commit id".into());
    }
    let object = format!("{commit}^{{commit}}");
    let status = Command::new("git")
        .current_dir(root)
        .args(["cat-file", "-e", &object])
        .status()
        .map_err(|error| error.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("archive commit is not present: {commit}"))
    }
}

/// Creates a private temporary directory without adding a runtime dependency.
fn temporary_directory(label: &str) -> Result<PathBuf, String> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_nanos();
    let directory = env::temp_dir().join(format!(
        "agent-run-xtask-{label}-{}-{stamp}",
        std::process::id()
    ));
    fs::create_dir(&directory).map_err(|error| error.to_string())?;
    Ok(directory)
}

/// Runs git archive with the source-tree exclusions required by M56.
fn git_archive(
    root: &Path,
    commit: &str,
    output: &Path,
    prefix: &str,
    manifest: Option<&str>,
) -> Result<(), String> {
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let prefix_argument = format!("{prefix}/");
    let output_argument = output.to_str().ok_or("archive output is not valid UTF-8")?;
    let mut command = Command::new("git");
    command.current_dir(root).args([
        "archive",
        "--format=tar",
        "--prefix",
        &prefix_argument,
        "--output",
        output_argument,
        commit,
    ]);
    if let Some(manifest) = manifest {
        command.arg(format!("--add-virtual-file={prefix}/{MANIFEST}:{manifest}"));
    }
    command.args([
        "--",
        ".",
        ":(exclude)target/**",
        ":(exclude).cargo-home/**",
        ":(exclude).wt/**",
    ]);
    let output = command.output().map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "git archive failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

/// Extracts a tar archive into a newly created directory.
fn extract(archive: &Path, destination: &Path) -> Result<(), String> {
    fs::create_dir_all(destination).map_err(|error| error.to_string())?;
    let status = Command::new("tar")
        .args([
            "-xf",
            archive.to_str().ok_or("archive path is not valid UTF-8")?,
            "-C",
        ])
        .arg(destination)
        .status()
        .map_err(|error| error.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("tar extraction failed for {}", archive.display()))
    }
}

/// Walks extracted archive content and hashes files and symbolic-link targets.
fn records(root: &Path) -> Result<Vec<FileRecord>, String> {
    let mut result = Vec::new();
    walk_records(root, root, &mut result)?;
    result.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(result)
}

/// Recurses through one archive directory while preserving relative paths.
fn walk_records(root: &Path, directory: &Path, result: &mut Vec<FileRecord>) -> Result<(), String> {
    for entry in fs::read_dir(directory).map_err(|error| error.to_string())? {
        let path = entry.map_err(|error| error.to_string())?.path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        if metadata.is_dir() {
            walk_records(root, &path, result)?;
        } else if metadata.is_file() {
            let bytes = fs::read(&path).map_err(|error| error.to_string())?;
            result.push(FileRecord {
                path: path
                    .strip_prefix(root)
                    .map_err(|error| error.to_string())?
                    .to_path_buf(),
                bytes: bytes.len() as u64,
                sha256: release::digest_bytes(&bytes),
            });
        } else if metadata.file_type().is_symlink() {
            let target = fs::read_link(&path).map_err(|error| error.to_string())?;
            let bytes = target.to_string_lossy().as_bytes().to_vec();
            result.push(FileRecord {
                path: path
                    .strip_prefix(root)
                    .map_err(|error| error.to_string())?
                    .to_path_buf(),
                bytes: bytes.len() as u64,
                sha256: release::digest_bytes(&bytes),
            });
        } else {
            return Err(format!("unsupported archive entry: {}", path.display()));
        }
    }
    Ok(())
}

/// Parses and validates the file records in an archive manifest.
fn manifest_records(manifest: &Value) -> Result<BTreeMap<String, FileRecord>, String> {
    if manifest["format"].as_u64() != Some(1) {
        return Err("unsupported archive manifest format".into());
    }
    let entries = manifest["files"]
        .as_array()
        .ok_or("archive manifest has no files")?;
    let mut records = BTreeMap::new();
    for entry in entries {
        let path = relative_path(entry["path"].as_str().ok_or("archive file has no path")?)?;
        let name = path.to_string_lossy().into_owned();
        let record = FileRecord {
            path,
            bytes: entry["bytes"]
                .as_u64()
                .ok_or("archive file has invalid byte count")?,
            sha256: valid_hash(
                entry["sha256"]
                    .as_str()
                    .ok_or("archive file has no sha256")?,
            )?,
        };
        if records.insert(name.clone(), record).is_some() {
            return Err(format!("duplicate archive manifest path: {name}"));
        }
    }
    Ok(records)
}

/// Validates the lock object against the already parsed file records.
fn verify_lock(manifest: &Value, files: &BTreeMap<String, FileRecord>, errors: &mut Vec<String>) {
    let lock = &manifest["lock"];
    let Some(path) = lock["path"].as_str() else {
        errors.push("archive manifest has no lock entry".into());
        return;
    };
    if path != "Cargo.lock" {
        errors.push(format!("archive lock path is not Cargo.lock: {path}"));
    }
    let Some(record) = files.get(path) else {
        errors.push(format!("archive lock is not listed: {path}"));
        return;
    };
    if lock["bytes"].as_u64() != Some(record.bytes) {
        errors.push("archive lock byte count does not match its file".into());
    }
    if lock["sha256"].as_str() != Some(record.sha256.as_str()) {
        errors.push("archive lock hash does not match its file".into());
    }
}

/// Rejects absolute and parent-traversing manifest paths.
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
        return Err(format!("unsafe archive manifest path: {}", path.display()));
    }
    Ok(path.to_path_buf())
}

/// Accepts only the lowercase hexadecimal digest form used by release manifests.
fn valid_hash(hash: &str) -> Result<String, String> {
    if hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(hash.to_owned())
    } else {
        Err("archive manifest has an invalid sha256".into())
    }
}

#[cfg(test)]
mod tests {
    use super::relative_path;
    use std::path::Path;

    /// Confirms archive manifests cannot address files outside their root.
    #[test]
    fn rejects_unsafe_manifest_paths() {
        assert!(relative_path("../Cargo.lock").is_err());
        assert!(relative_path("/tmp/Cargo.lock").is_err());
        assert_eq!(
            relative_path("Cargo.lock").expect("safe path"),
            Path::new("Cargo.lock")
        );
    }
}
