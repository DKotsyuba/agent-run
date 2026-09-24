//! Standalone installation wrapper around the existing sealed-release deployer.

use crate::{deploy, release};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Component, Path, PathBuf},
    time::Duration,
};

/// Parses the deliberately small, non-forcing standalone installer interface.
pub fn command(arguments: &[String]) -> Result<(), String> {
    let mut values = BTreeMap::new();
    for pair in arguments.chunks(2) {
        if pair.len() != 2
            || !["--release", "--prefix", "--home", "--bin-dir", "--version"]
                .contains(&pair[0].as_str())
            || values.insert(pair[0].as_str(), pair[1].as_str()).is_some()
        {
            return Err(
                "expected --release DIR --prefix DIR --home DIR --bin-dir DIR --version X.Y.Z"
                    .into(),
            );
        }
    }
    let value = |name| {
        values
            .get(name)
            .copied()
            .ok_or_else(|| format!("{name} is required"))
    };
    install(
        Path::new(value("--release")?),
        Path::new(value("--prefix")?),
        Path::new(value("--home")?),
        Path::new(value("--bin-dir")?),
        value("--version")?,
    )
}

/// Acquires a private advisory lock without following an existing symlink.
fn lock(path: &Path, message: &str) -> Result<File, String> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.try_lock_exclusive().map_err(|_| message.to_owned())?;
    Ok(file)
}

/// Creates a private installation directory and rejects ambiguous relative destinations.
fn directory(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute()
        || path.parent().is_none()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(
            "installation paths must be absolute, non-root directories without '..'".into(),
        );
    }
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
        .map_err(|e| e.to_string())?;
    fs::canonicalize(path).map_err(|error| error.to_string())
}

/// Quotes one literal path for the generated POSIX launcher, including embedded apostrophes.
fn quote(path: &Path) -> Result<String, String> {
    let path = path.to_str().ok_or("installation paths must be UTF-8")?;
    Ok(format!("'{}'", path.replace('\'', "'\\''")))
}

/// Copies only regular files and directories; writes the seal marker last.
fn copy_release(source: &Path, destination: &Path) -> Result<(), String> {
    for entry in fs::read_dir(source).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        if entry.file_name() == "COMPLETE" {
            continue;
        }
        let target = destination.join(entry.file_name());
        let kind = entry.file_type().map_err(|error| error.to_string())?;
        if kind.is_dir() {
            fs::create_dir(&target).map_err(|error| error.to_string())?;
            copy_release(&entry.path(), &target)?;
        } else if kind.is_file() {
            fs::copy(entry.path(), target).map_err(|error| error.to_string())?;
        } else {
            return Err("release contains a symlink or special file".into());
        }
    }
    if let Ok(metadata) = fs::symlink_metadata(source.join("COMPLETE")) {
        if !metadata.file_type().is_file() {
            return Err("release seal must be a regular file".into());
        }
        fs::copy(source.join("COMPLETE"), destination.join("COMPLETE"))
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Installs a verified immutable release, preserving data and refusing active writers.
///
/// The caller downloads into a disposable directory. This copies it into the
/// permanent prefix before using the journalled deployer. Broker and SQLite
/// writer locks stay held through backup, pointer switch and launcher placement.
/// No schema migration, service control, model setup or credential change occurs.
pub fn install(
    candidate: &Path,
    prefix: &Path,
    home: &Path,
    bin: &Path,
    version: &str,
) -> Result<(), String> {
    if version.split('.').count() != 3
        || version
            .split('.')
            .any(|part| part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err("version must be X.Y.Z".into());
    }
    release::verify(candidate)?;
    let metadata: serde_json::Value = serde_json::from_slice(
        &fs::read(candidate.join("metadata.json")).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    if metadata["version"].as_str() != Some(version) {
        return Err("release version does not match requested version".into());
    }
    let prefix = directory(prefix)?;
    let home = directory(home)?;
    let bin = directory(bin)?;
    let _install_lock = lock(
        &prefix.join(".install.lock"),
        "another installation is in progress",
    )?;
    if prefix.join("deploy.json").exists() {
        let journal: serde_json::Value = serde_json::from_slice(
            &fs::read(prefix.join("deploy.json")).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        if journal["phase"] == "prepared" {
            return Err("unfinished deployment: run release recover with this prefix and home before installing".into());
        }
    }
    let launcher = bin.join("agent-run");
    let target = prefix.join("current/bin/agent-run");
    let wrapper = format!("#!/bin/sh\n# agent-run managed launcher v1\nif [ -z \"${{AGENT_RUN_HOME:-}}\" ]; then AGENT_RUN_HOME={}; fi\nexport AGENT_RUN_HOME\nexec {} \"$@\"\n", quote(&home)?, quote(&target)?);
    match fs::symlink_metadata(&launcher) {
        Ok(metadata)
            if metadata.file_type().is_symlink()
                && fs::read_link(&launcher).ok().as_ref() == Some(&target) => {}
        Ok(metadata)
            if metadata.is_file()
                && fs::read(&launcher).ok().as_deref() == Some(wrapper.as_bytes()) => {}
        Ok(_) => {
            return Err(format!(
                "refusing to replace an unowned launcher: {}",
                launcher.display()
            ))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.to_string()),
    }
    let releases = directory(&prefix.join("releases"))?;
    let destination = releases.join(version);
    if fs::symlink_metadata(&destination).is_ok() {
        if fs::symlink_metadata(&destination)
            .map_err(|e| e.to_string())?
            .file_type()
            .is_symlink()
        {
            return Err("release destination must not be a symlink".into());
        }
        release::verify(&destination)?;
        if fs::read(destination.join("SHA256SUMS")).map_err(|e| e.to_string())?
            != fs::read(candidate.join("SHA256SUMS")).map_err(|e| e.to_string())?
        {
            return Err(
                "refusing to overwrite a different immutable release with the same version".into(),
            );
        }
    }
    let already_selected = destination.exists()
        && fs::canonicalize(prefix.join("current")).ok().as_ref() == Some(&destination);
    let mut launcher_file = tempfile::NamedTempFile::new_in(&bin).map_err(|e| e.to_string())?;
    launcher_file
        .write_all(wrapper.as_bytes())
        .map_err(|e| e.to_string())?;
    launcher_file
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o755))
        .map_err(|e| e.to_string())?;
    launcher_file
        .as_file()
        .sync_all()
        .map_err(|e| e.to_string())?;
    if !already_selected {
        let _broker = lock(
            &home.join(".api.sock.lock"),
            "stop the resident broker before updating; no processes were stopped",
        )?;
        let database = home.join("state.db");
        let _writer = if database.exists() {
            let connection =
                Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_WRITE)
                    .map_err(|e| e.to_string())?;
            connection
                .busy_timeout(Duration::from_millis(100))
                .map_err(|e| e.to_string())?;
            connection
                .execute_batch("BEGIN IMMEDIATE")
                .map_err(|e| format!("stop other state writers before updating: {e}"))?;
            let schema: u64 = connection
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .map_err(|e| e.to_string())?;
            if schema != release::schema_version(candidate)? {
                return Err("state schema differs from release; explicit migration is required before installation".into());
            }
            Some(connection)
        } else {
            None
        };
        deploy::quiescent(&home, false)?;
        if home.join("config.toml").exists() {
            agent_run_config::provider_config::ProviderConfig::load(&home).map_err(|e| {
                format!(
                    "configuration is incompatible; migrate it explicitly before installing: {e}"
                )
            })?;
        }
        if !destination.exists() {
            let staging = tempfile::Builder::new()
                .prefix(".install-")
                .tempdir_in(&releases)
                .map_err(|e| e.to_string())?;
            copy_release(candidate, staging.path())?;
            release::verify(staging.path())?;
            fs::rename(staging.path(), &destination).map_err(|e| e.to_string())?;
        }
        deploy::deploy(&prefix, &home, &destination, false)?;
        launcher_file
            .persist(&launcher)
            .map_err(|e| e.to_string())?;
    } else {
        launcher_file
            .persist(&launcher)
            .map_err(|e| e.to_string())?;
    }
    println!("agent-run {version} selected at {}\nLauncher: {}\nStart or restart your configured services explicitly.", destination.display(), launcher.display());
    Ok(())
}
