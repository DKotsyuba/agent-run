//! Immutable native release directory creation and manifest verification.

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
    agent_run_platform::fs::sha256(bytes)
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

/// Creates a sealed legacy release from an already-built native binary.
///
/// The resulting `releases/<version>` contains the binary, external collectors, metadata,
/// SHA256SUMS and COMPLETE marker. Existing complete releases are verified and
/// reused; incomplete candidates are rejected to retain forensic evidence.
/// This shape bundles no `agent-run-tui` observer; deploy fixtures and the
/// legacy-installability guarantees rely on it staying available.
pub fn build(output: &Path, version: &str, binary: &Path) -> Result<PathBuf, String> {
    build_inner(output, version, binary, None, None)
}

/// Seals a native release with the standalone deployment helper required by install.sh.
///
/// `tui` is the already-built `agent-run-tui` observer binary. When given, it
/// is sealed as `bin/agent-run-tui` from the same workspace version, and a
/// reused release directory that predates the bundled observer is rejected so
/// the version cannot silently lose its second executable.
pub fn build_with_installer(
    output: &Path,
    version: &str,
    binary: &Path,
    installer: &Path,
    tui: Option<&Path>,
) -> Result<PathBuf, String> {
    if !installer.is_file() {
        return Err("native release requires a built deployment helper".into());
    }
    let release = build_inner(output, version, binary, Some(installer), tui)?;
    if !release.join("bin/agent-run-deploy").is_file() {
        return Err("existing release predates install.sh; choose a new version".into());
    }
    if tui.is_some() && !release.join("bin/agent-run-tui").is_file() {
        return Err("existing release predates the bundled TUI; choose a new version".into());
    }
    Ok(release)
}

/// Writes a release once, optionally including the deployment helper and observer binaries.
///
/// `binary` is sealed as `bin/agent-run`; `installer`, when present, as
/// `bin/agent-run-deploy`; `tui`, when present, as `bin/agent-run-tui`. Each
/// input must already be a regular file, checked before the release directory
/// is created so a bad argument never leaves an incomplete candidate. Existing
/// sealed versions are reused only for the complete expected binary/asset and
/// metadata inventory. Conflicts require a new version; existing releases are
/// verified and compared read-only, never updated during candidate preparation.
fn build_inner(
    output: &Path,
    version: &str,
    binary: &Path,
    installer: Option<&Path>,
    tui: Option<&Path>,
) -> Result<PathBuf, String> {
    if version.trim().is_empty() || version.contains('/') {
        return Err("version must be a nonblank path component".into());
    }
    if !binary.is_file() {
        return Err(format!("binary is not a file: {}", binary.display()));
    }
    if let Some(tui) = tui
        && !tui.is_file()
    {
        return Err(format!(
            "bundled TUI binary is not a file: {}",
            tui.display()
        ));
    }
    let scripts_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../scripts");
    let metadata = format!(
        "{{\"version\":{version:?},\"format\":1,\"schema_version\":{SUPPORTED_SCHEMA_VERSION}}}\n"
    );
    let mut expected = std::collections::BTreeMap::new();
    for (source, name) in [
        (Some(binary), "agent-run"),
        (installer, "agent-run-deploy"),
        (tui, "agent-run-tui"),
    ] {
        if let Some(source) = source {
            expected.insert(
                PathBuf::from("bin").join(name),
                digest(source).map_err(|_| "input digest unavailable")?,
            );
        }
    }
    for directory in ["collectors", "services"] {
        let scripts = scripts_root.join(directory);
        for relative in files(&scripts).map_err(|_| "source asset inventory unavailable")? {
            expected.insert(
                PathBuf::from(directory).join(&relative),
                digest(&scripts.join(relative)).map_err(|_| "source asset digest unavailable")?,
            );
        }
    }
    expected.insert(
        PathBuf::from("metadata.json"),
        digest_bytes(metadata.as_bytes()),
    );
    let release = output.join("releases").join(version);
    if release.exists() {
        verify(&release)?;
        if tui.is_some() && !release.join("bin/agent-run-tui").is_file() {
            return Err("existing release predates the bundled TUI; choose a new version".into());
        }
        let actual = files(&release)
            .map_err(|_| "existing release inventory unavailable")?
            .into_iter()
            .filter(|path| path != Path::new("SHA256SUMS") && path != Path::new("COMPLETE"))
            .map(|path| {
                digest(&release.join(&path))
                    .map(|hash| (path, hash))
                    .map_err(|_| "existing release asset digest unavailable")
            })
            .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
        if actual != expected {
            return Err(
                "same-version release assets/metadata conflict; choose a new version".into(),
            );
        }
        return Ok(release);
    }
    fs::create_dir_all(release.join("bin")).map_err(|error| error.to_string())?;
    fs::copy(binary, release.join("bin/agent-run")).map_err(|error| error.to_string())?;
    if let Some(installer) = installer {
        fs::copy(installer, release.join("bin/agent-run-deploy"))
            .map_err(|error| error.to_string())?;
    }
    if let Some(tui) = tui {
        fs::copy(tui, release.join("bin/agent-run-tui")).map_err(|error| error.to_string())?;
    }
    // External integration scripts remain ordinary files, never embedded executable logic.
    for directory in ["collectors", "services"] {
        let scripts = scripts_root.join(directory);
        fs::create_dir(release.join(directory)).map_err(|error| error.to_string())?;
        for relative in files(&scripts).map_err(|error| error.to_string())? {
            let destination = release.join(directory).join(&relative);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent).map_err(|error| error.to_string())?;
            }
            fs::copy(scripts.join(&relative), destination).map_err(|error| error.to_string())?;
        }
    }

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

/// Validates the historical packaging seal before a current-target switch.
/// Metadata must be a regular file within 64 KiB before the legacy JSON parser
/// runs. COMPLETE remains a packaging marker, never an installation receipt.
pub fn verify(release: &Path) -> Result<(), String> {
    crate::delivery::read_regular(&release.join("metadata.json"), 65536)?;
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

    /// Separately sealed older collector/service/metadata variants remain readable
    /// but cannot be reused as this source's same-version complete candidate.
    #[test]
    fn same_version_changed_assets_are_not_a_noop() {
        for changed in [
            "collectors/codex.sh",
            "services/codegraph-probe.cjs",
            "metadata.json",
        ] {
            let temp = tempdir().unwrap();
            let binary = temp.path().join("binary");
            fs::write(&binary, b"same binary").unwrap();
            let release = build(temp.path(), "9.9.9", &binary).unwrap();
            if changed == "metadata.json" {
                let bytes = fs::read(release.join(changed)).unwrap();
                fs::write(
                    release.join(changed),
                    [b" \n".as_slice(), bytes.as_slice()].concat(),
                )
                .unwrap();
            } else {
                fs::write(release.join(changed), b"older fixture asset").unwrap();
            }
            let sums = super::files(&release)
                .unwrap()
                .into_iter()
                .filter(|path| {
                    path != std::path::Path::new("SHA256SUMS")
                        && path != std::path::Path::new("COMPLETE")
                })
                .map(|path| {
                    format!(
                        "{}  {}\n",
                        super::digest(&release.join(&path)).unwrap(),
                        path.display()
                    )
                })
                .collect::<String>();
            fs::write(release.join("SHA256SUMS"), sums).unwrap();
            verify(&release).unwrap();
            let before = fs::read(release.join(changed)).unwrap();
            assert!(
                build(temp.path(), "9.9.9", &binary).is_err(),
                "different sealed {changed} must require a new version"
            );
            assert_eq!(fs::read(release.join(changed)).unwrap(), before);
            verify(&release).unwrap();
        }
    }
}
