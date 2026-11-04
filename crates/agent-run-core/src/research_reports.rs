//! Authenticated research report publication; no command or general filesystem API.

use crate::{Error, Result, fs, service::ProviderLaunchIdentity, state::Store};
use agent_run_config::role_plan::ResolvedRolePlan;
use agent_run_domain::worker::{SaveReportReceipt, SaveReportRequest, WorkerToolCall};
use std::{io::Read, os::unix::fs::MetadataExt, path::Path};

/// Saves one report only for an authenticated, running research attempt.
/// The directory is frozen workdir authority, never a model argument. Every
/// path component is opened without following links; only an owned directory
/// and a validated flat report basename are accepted. Publication is atomic
/// and create-only; an identical existing regular file is a duplicate, while
/// different contents, symlinks, directories and special entries are refused.
/// No report contents or capability secrets enter event metadata. Validation,
/// authorization, integrity and I/O errors propagate; a lost receipt can be
/// reconciled by retrying the same filename/content. No process is spawned.
/// The durable `research_report_saved` event is recorded by the caller through
/// the writer; this function only reads the store.
pub(crate) fn save(
    store: &Store,
    call: &WorkerToolCall,
    input: &SaveReportRequest,
) -> Result<SaveReportReceipt> {
    input.validate()?;
    let auth = agent_run_store::worker::authenticate_attempt(
        &store.conn,
        &call.run_id,
        &call.attempt_id,
        &call.token,
        crate::domain::now(),
    )?
    .ok_or_else(|| Error::Validation("invalid worker capability".into()))?;
    if !auth.running {
        return Err(Error::Validation(
            "report writer requires a running attempt".into(),
        ));
    }
    let row = store.get(&call.run_id)?;
    let identity = ProviderLaunchIdentity::read(&row)?;
    let role = ResolvedRolePlan::from_payload(&identity.authority.role_payload)?;
    if !role.research_tools_only() {
        return Err(Error::Unsupported(
            "report writer requires research authority".into(),
        ));
    }
    let root = &identity.authority.workdir;
    if !root.is_absolute() || root.canonicalize()? != *root {
        return Err(Error::Validation(
            "report directory must be existing, canonical and without symlink aliases".into(),
        ));
    }
    let directory = fs::Dir::open_searchable(root)?;
    // SAFETY: geteuid reads this process's effective identity and has no side effects.
    if directory.entry(None)?.uid != unsafe { libc::geteuid() } {
        return Err(Error::Validation(
            "report directory is not owned by this user".into(),
        ));
    }
    let name = Path::new(&input.filename);
    let digest = fs::sha256(input.content.as_bytes());
    let duplicate = match directory.entry_type(name) {
        Ok(fs::EntryType::File) => {
            require_identical(&directory, name, &digest)?;
            true
        }
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            let temporary = format!(".agent-run-report-{}.tmp", uuid::Uuid::new_v4().simple());
            let temporary = Path::new(&temporary);
            directory.write(temporary, input.content.as_bytes(), 0o600)?;
            let published = directory.rename_entry_no_replace(temporary, name);
            let duplicate = match published {
                Ok(true) => false,
                Ok(false) => {
                    let _ = directory.remove(temporary);
                    require_identical(&directory, name, &digest)?;
                    true
                }
                Err(error) => {
                    let _ = directory.remove(temporary);
                    return Err(error);
                }
            };
            require_identical(&directory, name, &digest)?;
            duplicate
        }
        _ => {
            return Err(Error::Validation(
                "report target must not be a symlink or non-file entry".into(),
            ));
        }
    };
    let receipt = SaveReportReceipt {
        filename: input.filename.clone(),
        bytes: input.content.len() as u64,
        sha256: digest,
        duplicate,
    };
    Ok(receipt)
}

/// Verifies owner, type and digest through the same no-follow file descriptor.
/// Refuses an unsafe or different file; existing bytes never enter errors.
fn require_identical(directory: &fs::Dir, name: &Path, digest: &str) -> Result<()> {
    verify_opened(directory.open_file(name)?, digest)
}

/// Verifies the exact descriptor even if its previous pathname is replaced.
fn verify_opened(file: std::fs::File, digest: &str) -> Result<()> {
    let metadata = file.metadata()?;
    let unsafe_target = || {
        Error::Validation(
            "report target exists with different content or an unsafe type; overwrite refused"
                .into(),
        )
    };
    // SAFETY: geteuid reads process identity without mutation.
    let uid = unsafe { libc::geteuid() };
    if !metadata.is_file() || metadata.uid() != uid || metadata.len() > 65536 {
        return Err(unsafe_target());
    }
    let mut bytes = Vec::new();
    file.take(65537).read_to_end(&mut bytes)?;
    if bytes.len() > 65536 || fs::sha256(&bytes) != digest {
        return Err(unsafe_target());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Replacing a pathname after open cannot change which file is verified.
    #[test]
    fn duplicate_check_pins_the_open_file() {
        let temp = tempfile::tempdir().unwrap();
        let directory = fs::Dir::open(temp.path()).unwrap();
        directory
            .write(Path::new("report.md"), b"original", 0o600)
            .unwrap();
        let opened = directory.open_file(Path::new("report.md")).unwrap();
        std::fs::rename(temp.path().join("report.md"), temp.path().join("old.md")).unwrap();
        std::os::unix::fs::symlink("old.md", temp.path().join("report.md")).unwrap();
        let digest = fs::sha256(b"original");
        verify_opened(opened, &digest).unwrap();
        assert!(require_identical(&directory, Path::new("report.md"), &digest).is_err());
    }
}
