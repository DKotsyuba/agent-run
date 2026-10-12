//! Cooperative cross-process writer exclusion using the existing fs2 dependency.

use fs2::FileExt;
use std::{
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

/// An exclusive writer lock held until this owned guard is dropped.
///
/// All cooperating writers and offline tools must use the same canonical lock
/// path. It does not block rogue SQLite clients and does not prove application
/// process quiescence. Never unlink an occupied lock file to acquire authority.
pub struct WriterLease {
    /// Kernel-owned file lock, released on process death or last guard drop.
    file: File,
    /// Canonical parent plus unchanged lock basename, for application comparison.
    path: PathBuf,
}

impl WriterLease {
    /// Acquire a private owned regular lock file without waiting or deleting it.
    ///
    /// The parent must exist. On Unix, O_NOFOLLOW refuses symlinks atomically;
    /// ownership and private permissions are checked on the opened inode.
    /// Another holder returns WouldBlock. This changes no SQLite setting.
    pub fn acquire(path: &Path) -> io::Result<Self> {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("lock parent absent"))?
            .canonicalize()?;
        let name = path
            .file_name()
            .ok_or_else(|| io::Error::other("lock name absent"))?;
        let path = parent.join(name);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let file = options.open(&path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::other("lock is not a regular file"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            // SAFETY: geteuid has no inputs or preconditions.
            let uid = unsafe { libc::geteuid() };
            if metadata.uid() != uid || metadata.mode() & 0o077 != 0 {
                return Err(io::Error::other("lock is not private and owned"));
            }
        }
        file.try_lock_exclusive()?;
        Ok(Self { file, path })
    }

    /// The canonical lock path; applications compare it to their selected home.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for WriterLease {
    /// Unlock this inode; never delete the shared lock path.
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}
