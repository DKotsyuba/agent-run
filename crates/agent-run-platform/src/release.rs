//! Sealed release directories: the store schema this workspace supports and
//! verification of a release's `COMPLETE`/`SHA256SUMS`/`metadata.json` seal.
//!
//! The release builder (`xtask`), deployment and the one-time config
//! migration all read release facts from here, so a binary's supported schema
//! has a single source: [`STORE_SCHEMA_VERSION`].

use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path},
};

/// The state database schema supported by binaries built from this workspace.
pub const STORE_SCHEMA_VERSION: i64 = 18;

/// Lowercase SHA-256 of one release file.
fn digest(path: &Path) -> Result<String, String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(fs::read(path).map_err(|error| error.to_string())?)
    ))
}

/// Validates a sealed release: `COMPLETE` is exact, every `SHA256SUMS` entry
/// is a safe unique relative path whose bytes match, the manifest covers
/// `bin/agent-run` and `metadata.json`, and the metadata names its schema.
pub fn verify(release: &Path) -> Result<(), String> {
    if fs::read_to_string(release.join("COMPLETE")).map_err(|error| error.to_string())?
        != "complete\n"
    {
        return Err("release is incomplete".into());
    }
    let manifest =
        fs::read_to_string(release.join("SHA256SUMS")).map_err(|error| error.to_string())?;
    let mut seen = BTreeSet::new();
    for line in manifest.lines() {
        let (hash, name) = line.split_once("  ").ok_or("invalid sealed manifest")?;
        let relative = Path::new(name);
        if hash.len() != 64
            || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            || relative.is_absolute()
            || relative
                .components()
                .any(|part| matches!(part, Component::ParentDir))
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
