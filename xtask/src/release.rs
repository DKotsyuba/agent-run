//! Immutable native release directory creation and manifest verification.

use sha2::{Digest, Sha256};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// Records the store schema supported by binaries produced by this workspace:
/// the store's own current schema, never a separately maintained number.
pub(crate) const SUPPORTED_SCHEMA_VERSION: u64 =
    agent_run_platform::release::STORE_SCHEMA_VERSION as u64;

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
/// The resulting `releases/<version>` contains the binary, external collectors, metadata,
/// SHA256SUMS and COMPLETE marker. Existing complete releases are verified and
/// reused; incomplete candidates are rejected to retain forensic evidence.
pub fn build(output: &Path, version: &str, binary: &Path) -> Result<PathBuf, String> {
    build_inner(output, version, binary, None)
}

/// Seals a native release with the standalone deployment helper required by install.sh.
pub fn build_with_installer(
    output: &Path,
    version: &str,
    binary: &Path,
    installer: &Path,
) -> Result<PathBuf, String> {
    if !installer.is_file() {
        return Err("native release requires a built deployment helper".into());
    }
    let release = build_inner(output, version, binary, Some(installer))?;
    if !release.join("bin/agent-run-deploy").is_file() {
        return Err("existing release predates install.sh; choose a new version".into());
    }
    Ok(release)
}

/// Writes a release once, optionally including the native installation entry point.
fn build_inner(
    output: &Path,
    version: &str,
    binary: &Path,
    installer: Option<&Path>,
) -> Result<PathBuf, String> {
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
    if let Some(installer) = installer {
        fs::copy(installer, release.join("bin/agent-run-deploy"))
            .map_err(|error| error.to_string())?;
    }
    // External integration scripts remain ordinary files, never embedded executable logic.
    for directory in ["collectors", "services"] {
        let scripts = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../scripts")
            .join(directory);
        fs::create_dir(release.join(directory)).map_err(|error| error.to_string())?;
        for relative in files(&scripts).map_err(|error| error.to_string())? {
            let destination = release.join(directory).join(&relative);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent).map_err(|error| error.to_string())?;
            }
            fs::copy(scripts.join(&relative), destination).map_err(|error| error.to_string())?;
        }
    }
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
    agent_run_platform::release::verify(release)
}

/// Reads the schema version recorded by a sealed release.
pub(crate) fn schema_version(release: &Path) -> Result<u64, String> {
    agent_run_platform::release::schema_version(release)
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
        assert!(release.join("collectors/codex.sh").is_file());
        assert!(release.join("collectors/glm.jq").is_file());
        assert!(release.join("services/codegraph-probe.cjs").is_file());
        assert_eq!(
            super::schema_version(&release).expect("metadata schema"),
            agent_run_platform::release::STORE_SCHEMA_VERSION as u64,
            "release metadata records the store's current schema"
        );
        fs::write(release.join("bin/agent-run"), "changed").expect("tamper fixture");
        assert!(verify(&release).is_err());
    }
}
