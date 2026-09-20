//! Immutable native release directory creation and manifest verification.

use sha2::{Digest, Sha256};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// Records the store schema supported by binaries produced by this workspace.
pub(crate) const SUPPORTED_SCHEMA_VERSION: u64 = 16;

/// Returns a lowercase SHA-256 digest for arbitrary bytes.
pub(crate) fn digest_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Returns a lowercase SHA-256 digest for one regular release file.
pub(crate) fn digest(path: &Path) -> io::Result<String> {
    Ok(digest_bytes(&fs::read(path)?))
}

/// Walks regular files below `root`, returning normalized relative paths.
fn files(root: &Path) -> io::Result<Vec<PathBuf>> {
    /// Recurses through one immutable release directory.
    fn visit(root: &Path, directory: &Path, result: &mut Vec<PathBuf>) -> io::Result<()> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, result)?;
            } else if path.is_file() {
                result.push(path.strip_prefix(root).expect("descendant").to_path_buf());
            }
        }
        Ok(())
    }
    let mut result = Vec::new();
    visit(root, root, &mut result)?;
    result.sort();
    Ok(result)
}

/// Creates a sealed release from an already-built native binary.
///
/// The resulting `releases/<version>` contains only the binary, metadata,
/// SHA256SUMS and COMPLETE marker. Existing complete releases are verified and
/// reused; incomplete candidates are rejected to retain forensic evidence.
pub fn build(output: &Path, version: &str, binary: &Path) -> Result<PathBuf, String> {
    if version.trim().is_empty() || version.contains('/') {
        return Err("version must be a nonblank path component".into());
    }
    if !binary.is_file() {
        return Err(format!("binary is not a file: {}", binary.display()));
    }
    let release = output.join("releases").join(version);
    if release.exists() {
        verify(&release)?;
        return Ok(release);
    }
    fs::create_dir_all(release.join("bin")).map_err(|error| error.to_string())?;
    fs::copy(binary, release.join("bin/agent-run")).map_err(|error| error.to_string())?;
    let metadata = format!(
        "{{\"version\":{version:?},\"format\":1,\"schema_version\":{SUPPORTED_SCHEMA_VERSION}}}\n"
    );
    fs::write(release.join("metadata.json"), metadata).map_err(|error| error.to_string())?;
    let manifest = files(&release)
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|relative| {
            let path = release.join(&relative);
            Ok(format!(
                "{}  {}\n",
                digest(&path).map_err(|error| error.to_string())?,
                relative.display()
            ))
        })
        .collect::<Result<String, String>>()?;
    fs::write(release.join("SHA256SUMS"), manifest).map_err(|error| error.to_string())?;
    fs::write(release.join("COMPLETE"), "complete\n").map_err(|error| error.to_string())?;
    verify(&release)?;
    Ok(release)
}

/// Validates a sealed release before it can become a `current` target.
pub fn verify(release: &Path) -> Result<(), String> {
    if fs::read_to_string(release.join("COMPLETE")).map_err(|error| error.to_string())?
        != "complete\n"
    {
        return Err("release is incomplete".into());
    }
    let manifest =
        fs::read_to_string(release.join("SHA256SUMS")).map_err(|error| error.to_string())?;
    let mut seen = std::collections::BTreeSet::new();
    for line in manifest.lines() {
        let (hash, name) = line.split_once("  ").ok_or("invalid sealed manifest")?;
        let relative = Path::new(name);
        if hash.len() != 64
            || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            || relative.is_absolute()
            || relative
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
            || !seen.insert(name)
        {
            return Err("unsafe or duplicate sealed manifest path".into());
        }
        if digest(&release.join(relative)).map_err(|error| error.to_string())? != hash {
            return Err(format!("corrupt sealed release file: {name}"));
        }
    }
    if !seen.contains("bin/agent-run") || !seen.contains("metadata.json") {
        return Err("sealed manifest does not cover runtime entry points".into());
    }
    schema_version(release)?;
    Ok(())
}

/// Reads the schema version recorded by a sealed release.
pub(crate) fn schema_version(release: &Path) -> Result<u64, String> {
    let metadata: serde_json::Value = serde_json::from_slice(
        &fs::read(release.join("metadata.json")).map_err(|error| error.to_string())?,
    )
    .map_err(|error| format!("invalid release metadata: {error}"))?;
    metadata["schema_version"]
        .as_u64()
        .ok_or_else(|| "release metadata has no schema_version".into())
}

#[cfg(test)]
mod tests {
    use super::{build, verify};
    use std::fs;
    use tempfile::tempdir;

    /// Mirrors `tests/test_release_script.py` sealed manifest verification.
    #[test]
    fn python_release_script_seals_and_rejects_tampering() {
        let temporary = tempdir().expect("temporary directory");
        let binary = temporary.path().join("agent-run");
        fs::write(&binary, "native binary").expect("fixture binary");
        let release = build(temporary.path(), "0.12.0", &binary).expect("sealed release");
        verify(&release).expect("valid manifest");
        fs::write(release.join("bin/agent-run"), "changed").expect("tamper fixture");
        assert!(verify(&release).is_err());
    }
}
