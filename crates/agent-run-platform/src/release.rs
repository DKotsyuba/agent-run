//! Sealed release directories: the store schema this workspace supports and
//! verification of a release's `COMPLETE`/`SHA256SUMS`/`metadata.json` seal.
//!
//! The release builder (`xtask`), deployment and the one-time config
//! migration all read release facts from here, so a binary's supported schema
//! has a single source: [`STORE_SCHEMA_VERSION`].

use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

/// The state database schema supported by binaries built from this workspace.
pub const STORE_SCHEMA_VERSION: i64 = 32;

/// Lowercase SHA-256 of one release file.
fn digest(path: &Path) -> Result<String, String> {
    Ok(crate::fs::sha256(
        &fs::read(path).map_err(|error| error.to_string())?,
    ))
}

/// Files that seal a release and are therefore never listed in `SHA256SUMS`.
const SEAL_FILES: [&str; 2] = ["COMPLETE", "SHA256SUMS"];

/// Collects every regular file below `directory` as a `/`-separated path
/// relative to the release root, without following links.
///
/// Only directories and regular files are accepted; a symlink (dangling or
/// not) or any special file anywhere in the tree is refused so that no entry
/// can alias bytes outside the sealed inventory. Messages never name paths.
fn regular_files(
    root: &Path,
    directory: &Path,
    files: &mut BTreeSet<String>,
) -> Result<(), String> {
    for entry in fs::read_dir(directory).map_err(|error| error.to_string())? {
        let path = entry.map_err(|error| error.to_string())?.path();
        let kind = fs::symlink_metadata(&path)
            .map_err(|error| error.to_string())?
            .file_type();
        if kind.is_dir() {
            regular_files(root, &path, files)?;
        } else if kind.is_file() {
            let relative = path.strip_prefix(root).map_err(|error| error.to_string())?;
            files.insert(
                relative
                    .to_str()
                    .ok_or("release path is not UTF-8")?
                    .to_owned(),
            );
        } else {
            return Err("release contains a link or special file".into());
        }
    }
    Ok(())
}

/// Validates a sealed release: `COMPLETE` is exact, every `SHA256SUMS` entry
/// is a unique normalized relative path whose bytes match, the manifest covers
/// `bin/agent-run` and `metadata.json`, and the metadata names its schema.
///
/// The inventory is closed: the release holds only directories and regular
/// files, and every file except `COMPLETE` and `SHA256SUMS` is listed. An
/// added executable or asset, including an unlisted `bin/agent-run-tui`, is
/// refused, while a legacy release that never listed a TUI stays valid. This
/// proves local candidate integrity only; it is not an archive attestation.
pub fn verify(release: &Path) -> Result<(), String> {
    if fs::read_to_string(release.join("COMPLETE")).map_err(|error| error.to_string())?
        != "complete\n"
    {
        return Err("release is incomplete".into());
    }
    let manifest =
        fs::read_to_string(release.join("SHA256SUMS")).map_err(|error| error.to_string())?;
    let mut actual = BTreeSet::new();
    regular_files(release, release, &mut actual)?;
    let mut seen = BTreeSet::new();
    for line in manifest.lines() {
        let (hash, name) = line.split_once("  ").ok_or("invalid sealed manifest")?;
        let relative = Path::new(name);
        if hash.len() != 64
            || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            || name.is_empty()
            || SEAL_FILES.contains(&name)
            || !relative
                .components()
                .all(|part| matches!(part, Component::Normal(_)))
            || relative.components().collect::<PathBuf>().as_os_str() != relative.as_os_str()
            || !seen.insert(name)
        {
            return Err("unsafe or duplicate sealed manifest path".into());
        }
        if digest(&release.join(relative))? != hash {
            return Err(format!("corrupt sealed release file: {name}"));
        }
    }
    if !seen.contains("bin/agent-run") || !seen.contains("metadata.json") {
        return Err("sealed manifest does not cover runtime entry points".into());
    }
    if actual
        .iter()
        .any(|name| !SEAL_FILES.contains(&name.as_str()) && !seen.contains(name.as_str()))
    {
        return Err("release contains an unlisted file".into());
    }
    schema_version(release)?;
    Ok(())
}

/// Reads the store schema recorded in a release's `metadata.json`.
pub fn schema_version(release: &Path) -> Result<u64, String> {
    let metadata: serde_json::Value = serde_json::from_slice(
        &fs::read(release.join("metadata.json")).map_err(|error| error.to_string())?,
    )
    .map_err(|error| format!("invalid release metadata: {error}"))?;
    metadata["schema_version"]
        .as_u64()
        .ok_or_else(|| "release metadata has no schema_version".into())
}
