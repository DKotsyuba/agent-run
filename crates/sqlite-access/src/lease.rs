//! Cooperative cross-process writer exclusion using the existing fs2 dependency.

use fs2::FileExt;
use std::{
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// How long a starting resident owner waits for its predecessor's kernel locks.
///
/// A terminated process (SIGKILL, out-of-memory, crash) keeps its flocks until
/// the kernel has torn down its last thread, which can outlast the signal by
/// tens of milliseconds; an immediate successor must not mistake that for a
/// live owner. A live owner still refuses once this bound has passed.
pub const PREDECESSOR_SETTLE: Duration = Duration::from_secs(3);

/// Pause between two attempts while a predecessor's lock is still held.
pub const SETTLE_POLL: Duration = Duration::from_millis(20);

/// Take an exclusive flock, retrying for at most `wait` while it is held.
///
/// Only resident startup uses a nonzero `wait`; offline tools keep the
/// immediate refusal. The last WouldBlock is returned when the bound passes.
pub fn lock_exclusive_settled(file: &File, wait: Duration) -> io::Result<()> {
    let deadline = Instant::now() + wait;
    loop {
        match file.try_lock_exclusive() {
            Err(error) if is_held(&error) && Instant::now() < deadline => {
                std::thread::sleep(SETTLE_POLL.min(wait));
            }
            result => return result,
        }
    }
}

/// Whether a failed non-blocking flock reports another holder.
pub fn is_held(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
        || error.raw_os_error() == fs2::lock_contended_error().raw_os_error()
}

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
        Self::acquire_settled(path, Duration::ZERO)
    }

    /// As [`Self::acquire`], waiting at most `wait` for a terminated holder.
    pub fn acquire_settled(path: &Path, wait: Duration) -> io::Result<Self> {
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
        lock_exclusive_settled(&file, wait)?;
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

#[cfg(test)]
mod settle_tests {
    use super::*;

    /// Opens another description of one lock file, as a second process would.
    fn open(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap()
    }

    /// A lock released within the bound is acquired instead of refused.
    #[test]
    fn waits_for_a_releasing_holder() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lock");
        let holder = open(&path);
        holder.try_lock_exclusive().unwrap();
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(150));
            drop(holder);
        });
        let waiter = open(&path);
        assert!(is_held(&waiter.try_lock_exclusive().unwrap_err()));
        lock_exclusive_settled(&waiter, Duration::from_secs(5)).unwrap();
        release.join().unwrap();
    }

    /// A live holder still refuses after the bound, and a zero bound never waits.
    #[test]
    fn live_holder_refuses_after_the_bound() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lock");
        let holder = open(&path);
        holder.try_lock_exclusive().unwrap();
        let waiter = open(&path);
        let started = Instant::now();
        let error = lock_exclusive_settled(&waiter, Duration::from_millis(120)).unwrap_err();
        assert!(is_held(&error));
        assert!(started.elapsed() >= Duration::from_millis(120));
        let started = Instant::now();
        assert!(lock_exclusive_settled(&waiter, Duration::ZERO).is_err());
        assert!(started.elapsed() < Duration::from_millis(100));
    }
}
