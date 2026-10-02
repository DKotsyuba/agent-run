//! Descriptor-anchored local storage. No symlinks are followed inside an owned tree.
mod cloning;
use agent_run_domain::{Error, Result, error::invalid};
use sha2::{Digest, Sha256};
use std::{
    ffi::{CStr, CString, OsString},
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{OpenOptionsExt, PermissionsExt},
    },
    path::{Component, Path, PathBuf},
};

/// Hashes arbitrary bytes, including empty input, into 64 lowercase hexadecimal digits.
pub fn sha256(bytes: &[u8]) -> String {
    agent_run_domain::canonical::hex_digest(&Sha256::digest(bytes))
}
pub fn expand(path: &Path) -> Result<PathBuf> {
    let s = path.to_str().ok_or_else(|| invalid("path must be UTF-8"))?;
    let p = if s == "~" || s.starts_with("~/") {
        let h = std::env::var_os("HOME").ok_or_else(|| invalid("HOME is unavailable"))?;
        if s == "~" {
            PathBuf::from(h)
        } else {
            PathBuf::from(h).join(&s[2..])
        }
    } else {
        path.to_path_buf()
    };
    if !p.is_absolute() {
        return Err(invalid("configuration paths must be absolute"));
    }
    Ok(p)
}
pub fn home(path: Option<PathBuf>) -> Result<PathBuf> {
    let p = path
        .or_else(|| std::env::var_os("AGENT_RUN_HOME").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("~/.agent-run"));
    if p.as_os_str().is_empty() {
        return Err(invalid("agent-run home must not be blank"));
    }
    let p = if p.is_relative() && !p.to_string_lossy().starts_with('~') {
        std::env::current_dir()?.join(p)
    } else {
        expand(&p)?
    };
    if p.exists() {
        Ok(p.canonicalize()?)
    } else {
        Ok(p)
    }
}
pub fn private_dir(path: &Path) -> Result<()> {
    if let Ok(m) = std::fs::symlink_metadata(path)
        && (m.file_type().is_symlink() || !m.is_dir())
    {
        return Err(invalid("private home must be a real directory"));
    }
    std::fs::create_dir_all(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}
/// Accepts a directory synchronization that the filesystem reports unsupported.
///
/// `outcome` is the raw result of one directory `fsync`. `EINVAL` and `ENOTSUP`
/// mean the filesystem offers no directory synchronization at all, which Python
/// tolerates (`_fsync_directory_descriptor`, `adapters/home.py:122-129`) so a
/// publish is never refused by a filesystem capability. Every other error
/// propagates: a caller must not report a durable publish after an available
/// sync operation actually failed.
pub fn tolerate_unsupported_sync(outcome: std::io::Result<()>) -> Result<()> {
    match outcome {
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::EINVAL) | Some(libc::ENOTSUP)
            ) =>
        {
            Ok(())
        }
        other => Ok(other?),
    }
}
/// Persists one directory's entry changes through its open descriptor.
///
/// `directory` must be an open descriptor for the directory whose entries
/// changed. Returns once those entries are durable, or immediately when the
/// filesystem reports directory synchronization unsupported; see
/// [`tolerate_unsupported_sync`] for the exact tolerated errors. This is
/// std's `File::sync_all`: `fcntl(F_FULLFSYNC)` on Apple hosts (fsync(2)
/// plus a drain of the whole device write queue), `fsync(2)` elsewhere.
fn sync_directory(directory: &File) -> Result<()> {
    tolerate_unsupported_sync(directory.sync_all())
}

/// How far one namespace mutation flushes its parent directory.
///
/// On Apple hosts `fsync(2)` only moves an object's data and attributes to
/// the device, which may still reorder or lose them, while `F_FULLFSYNC`
/// also drains the device queue so that "data that had been fsync'd on the
/// same device before is guaranteed to be persisted when this call returns"
/// (`fcntl(2)`). A batch may therefore apply [`Flush::Skipped`] mutations,
/// push every changed object once with plain `fsync(2)` ([`push`],
/// [`Dir::push`], [`Dir::push_tree`]) and make them all durable with one
/// [`Dir::sync`] barrier on the same device. On other hosts `fsync(2)` is
/// already the durable operation, so that ordering never weakens a
/// non-Apple fallback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flush {
    /// Durable when the call returns (`File::sync_all`, the historical
    /// behavior of every synced primitive).
    Durable,
    /// Not flushed: either disposable content a crash may lose or leave
    /// partial, which its owner's recovery must discard or re-prove, or a
    /// batch whose caller pushes the changed directory and crosses a
    /// [`Dir::sync`] barrier before anything depends on it.
    Skipped,
}

/// Pushes one open file or directory to its device with plain `fsync(2)`.
///
/// Durable on return except on Apple hosts, where it becomes durable only
/// once a later `F_FULLFSYNC` ([`Dir::sync`]) on the same device returns;
/// see [`Flush`].
/// Filesystems reporting synchronization unsupported are tolerated like
/// [`tolerate_unsupported_sync`].
pub fn push(file: &File) -> Result<()> {
    // SAFETY: the descriptor is live for the duration of this call.
    let outcome = if unsafe { libc::fsync(file.as_raw_fd()) } < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    };
    tolerate_unsupported_sync(outcome)
}

/// Flushes one open directory descriptor to the requested [`Flush`] level.
fn flush(file: &File, level: Flush) -> Result<()> {
    match level {
        Flush::Durable => sync_directory(file),
        Flush::Skipped => Ok(()),
    }
}
pub fn relative(path: &Path) -> Result<Vec<CString>> {
    let mut result = Vec::new();
    for c in path.components() {
        match c {
            Component::Normal(v) => {
                use std::os::unix::ffi::OsStrExt;
                result.push(CString::new(v.as_bytes()).map_err(|_| invalid("NUL in path"))?);
            }
            _ => return Err(invalid("owned path must be relative without dot traversal")),
        }
    }
    if result.is_empty() {
        return Err(invalid("empty owned path"));
    }
    Ok(result)
}
/// A point in [`Dir::write_seamed`]'s temp-write/rename/parent-sync sequence
/// where a test can inject a simulated crash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    /// The temp file holds the full new bytes but is not yet synced or renamed.
    MidWrite,
    /// The temp file is synced to disk but not yet renamed onto `path`.
    BeforeRename,
    /// `path` now holds the new bytes; only the parent-directory sync is left.
    AfterRename,
}
/// A no-follow classification of one entry beneath an owned directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    /// A real directory.
    Directory,
    /// A regular file.
    File,
    /// A symbolic link, never dereferenced by this module.
    Symlink,
    /// A device, FIFO, socket, or other unsupported entry.
    Special,
}

/// One no-follow identity snapshot of an entry beneath an owned directory.
///
/// Captured through `fstatat`/`fstat` on a live descriptor, so every field
/// describes the entry this process actually opened rather than a path that
/// may have been swapped between checks. Retention uses it to prove type,
/// ownership and age before any removal, and `device`/`inode` to re-verify
/// the same object immediately before unlinking it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Entry {
    /// Classification of the final entry without following it.
    pub kind: EntryType,
    /// Whether the entry is a Unix socket, the one `Special` shape retention probes.
    pub socket: bool,
    /// Owning user id; only the effective user's entries are ever touched.
    pub uid: u32,
    /// Device half of the entry's inode identity.
    pub device: u64,
    /// Inode half of the entry's inode identity.
    pub inode: u64,
    /// Last modified time in Unix seconds, including available subseconds.
    pub modified: f64,
    /// Permission bits (`st_mode & 0o7777`) of the entry, without following it.
    pub mode: u32,
}

/// Converts one raw `stat` result into the public no-follow identity shape.
fn entry_of(status: &libc::stat) -> Entry {
    let kind = status.st_mode & libc::S_IFMT;
    Entry {
        kind: if kind == libc::S_IFDIR {
            EntryType::Directory
        } else if kind == libc::S_IFREG {
            EntryType::File
        } else if kind == libc::S_IFLNK {
            EntryType::Symlink
        } else {
            EntryType::Special
        },
        uid: status.st_uid,
        #[cfg(target_os = "macos")]
        device: status.st_dev as u64,
        #[cfg(not(target_os = "macos"))]
        device: status.st_dev,
        inode: status.st_ino,
        socket: kind == libc::S_IFSOCK,
        modified: status.st_mtime as f64 + status.st_mtime_nsec as f64 / 1e9,
        mode: (status.st_mode & 0o7777) as u32,
    }
}

pub struct Dir(File);

/// One live bounded scan with its own directory offset, reusable across broker passes.
pub struct DirScan {
    /// Unique libc directory stream, closed when the scan is dropped.
    stream: *mut libc::DIR,
}

// SAFETY: the broker stores each DIR pointer behind a mutex and never invokes
// readdir concurrently on one stream; ownership may move between workers.
unsafe impl Send for DirScan {}

impl Drop for DirScan {
    /// Closes the uniquely owned directory stream.
    fn drop(&mut self) {
        // SAFETY: this pointer came from one successful fdopendir.
        unsafe { libc::closedir(self.stream) };
    }
}

impl DirScan {
    /// Returns at most `limit` names and whether this live stream reached EOF.
    pub fn next_batch(&mut self, limit: usize) -> Result<(Vec<OsString>, bool)> {
        if limit == 0 || limit > 256 {
            return Err(invalid("invalid directory batch"));
        }
        let mut names = Vec::with_capacity(limit);
        while names.len() < limit {
            // SAFETY: errno is thread-local; clearing it distinguishes EOF from a read error.
            #[cfg(target_os = "macos")]
            unsafe {
                *libc::__error() = 0;
            }
            // SAFETY: Linux exposes the same thread-local errno through this accessor.
            #[cfg(any(target_os = "linux", target_os = "android"))]
            unsafe {
                *libc::__errno_location() = 0;
            }
            // SAFETY: this live DIR pointer is exclusively borrowed by the caller.
            let entry = unsafe { libc::readdir(self.stream) };
            if entry.is_null() {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error().is_some_and(|code| code != 0) {
                    return Err(error.into());
                }
                return Ok((names, true));
            }
            // SAFETY: d_name is NUL-terminated while the entry is live.
            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
            if name != b"." && name != b".." {
                use std::os::unix::ffi::OsStringExt;
                names.push(OsString::from_vec(name.to_vec()));
            }
        }
        Ok((names, false))
    }
}
impl Dir {
    pub fn open(path: &Path) -> Result<Self> {
        let f = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
            .open(path)?;
        Ok(Self(f))
    }
    /// Opens a new directory description so independent scans start at offset zero.
    pub fn scan(&self) -> Result<DirScan> {
        // SAFETY: openat of "." resolves beneath this already-open real directory.
        let raw = unsafe {
            libc::openat(
                self.0.as_raw_fd(),
                c".".as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if raw < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: fdopendir consumes the distinct descriptor on success.
        let stream = unsafe { libc::fdopendir(raw) };
        if stream.is_null() {
            // SAFETY: fdopendir did not take ownership on failure.
            unsafe { libc::close(raw) };
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(DirScan { stream })
    }
    /// Reads one bounded batch from the start using an independent stream.
    pub fn list_batch(&self, limit: usize) -> Result<(Vec<OsString>, bool)> {
        self.scan()?.next_batch(limit)
    }
    /// List a directory through its descriptor without following path links.
    ///
    /// `path` is relative to this directory; `None` lists this directory. The
    /// names are sorted bytewise and omit `.` and `..`.
    pub fn list(&self, path: Option<&Path>) -> Result<Vec<OsString>> {
        let file = match path {
            Some(path) => self.open_directory(path)?,
            None => self.0.try_clone()?,
        };
        // SAFETY: `into_raw_fd` transfers the duplicate descriptor to libc;
        // `closedir` below owns and closes it on every normal return.
        let raw = std::os::fd::IntoRawFd::into_raw_fd(file);
        // SAFETY: `raw` is a valid directory descriptor owned by this call.
        let directory = unsafe { libc::fdopendir(raw) };
        if directory.is_null() {
            // SAFETY: fdopendir did not take ownership on failure.
            unsafe { libc::close(raw) };
            return Err(std::io::Error::last_os_error().into());
        }
        let mut names = Vec::new();
        loop {
            // SAFETY: errno is thread-local; clearing it distinguishes EOF
            // from a readdir failure without observing any other thread.
            #[cfg(target_os = "macos")]
            unsafe {
                *libc::__error() = 0;
            }
            // SAFETY: errno is thread-local; see the macOS branch above.
            #[cfg(any(target_os = "linux", target_os = "android"))]
            unsafe {
                *libc::__errno_location() = 0;
            }
            // SAFETY: `directory` remains valid until closed below.
            let entry = unsafe { libc::readdir(directory) };
            if entry.is_null() {
                let error = std::io::Error::last_os_error();
                // SAFETY: closes the libc-owned descriptor exactly once.
                unsafe { libc::closedir(directory) };
                if error.raw_os_error().is_some_and(|code| code != 0) {
                    return Err(error.into());
                }
                break;
            }
            // SAFETY: d_name is NUL-terminated by readdir for this entry.
            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
            if name != b"." && name != b".." {
                use std::os::unix::ffi::OsStringExt;
                names.push(OsString::from_vec(name.to_vec()));
            }
        }
        names.sort();
        Ok(names)
    }
    /// Captures one entry's no-follow identity through this directory.
    ///
    /// `path` is relative to this directory; `None` stats the directory's own
    /// descriptor. The result never follows the final entry, and every parent
    /// component is opened with `O_NOFOLLOW`, matching [`Dir::list`]. An error
    /// means the entry's identity is unknown and callers must fail closed.
    pub fn entry(&self, path: Option<&Path>) -> Result<Entry> {
        let (parent, name) = match path {
            Some(path) => self.parent(path, false)?,
            None => {
                // SAFETY: stat is a plain C output buffer; fstat initializes it before use.
                let mut status = unsafe { std::mem::zeroed::<libc::stat>() };
                // SAFETY: the descriptor is live and owned by this Dir.
                if unsafe { libc::fstat(self.0.as_raw_fd(), &mut status) } < 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
                return Ok(entry_of(&status));
            }
        };
        // SAFETY: stat is a plain C output buffer; fstatat initializes it before use.
        let mut status = unsafe { std::mem::zeroed::<libc::stat>() };
        // SAFETY: descriptor/name are live and fstatat retains neither.
        if unsafe {
            libc::fstatat(
                parent.as_raw_fd(),
                name.as_ptr(),
                &mut status,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } < 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(entry_of(&status))
    }
    /// Remove one empty owned directory through its no-follow parent.
    ///
    /// The final component is unlinked with `AT_REMOVEDIR`, so a symlink or
    /// non-directory at that name is refused by the kernel rather than
    /// dereferenced; the parent directory is synced like [`Dir::remove`].
    /// Fails with `ENOTEMPTY` while the directory still holds entries.
    pub fn remove_directory(&self, path: &Path) -> Result<()> {
        self.unlink(path, libc::AT_REMOVEDIR, Flush::Durable)
    }
    /// [`Dir::remove_directory`] without the parent-directory durability
    /// flush ([`Flush::Skipped`]), for emptying a disposable tree whose
    /// removal a crash may leave partial and its owner's recovery removes
    /// again without proof; a later durable removal of the emptied parent
    /// is the only flush it needs.
    pub fn discard_directory(&self, path: &Path) -> Result<()> {
        self.unlink(path, libc::AT_REMOVEDIR, Flush::Skipped)
    }
    /// [`Dir::remove`] without the parent-directory durability flush, for
    /// emptying a disposable tree; see [`Dir::discard_directory`].
    pub fn discard(&self, path: &Path) -> Result<()> {
        self.unlink(path, 0, Flush::Skipped)
    }
    /// Unlinks one final component through its no-follow parent with
    /// `flags`, flushing the parent directory to `level`.
    fn unlink(&self, path: &Path, flags: libc::c_int, level: Flush) -> Result<()> {
        let (parent, name) = self.parent(path, false)?;
        // SAFETY: parent is a live directory descriptor and name is NUL-terminated.
        if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), flags) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        flush(&parent, level)
    }
    /// Flushes this directory durably and acts as the device barrier.
    ///
    /// On Apple hosts this is `F_FULLFSYNC`, which drains the device queue so
    /// every object earlier pushed with plain `fsync(2)` on the same device
    /// ([`push`], [`Dir::push`], [`Dir::push_tree`]) is persisted
    /// when it returns; elsewhere `fsync(2)`, where each push was already
    /// durable. It persists nothing that was never pushed: objects created
    /// with [`Flush::Skipped`] must be pushed first.
    pub fn sync(&self) -> Result<()> {
        sync_directory(&self.0)
    }
    /// Pushes this directory's own entries and attributes to the device
    /// with plain `fsync(2)`; see [`push`].
    pub fn push(&self) -> Result<()> {
        push(&self.0)
    }
    /// Pushes every regular file and directory beneath this directory, and
    /// this directory itself last, with plain `fsync(2)` through no-follow
    /// descriptors.
    ///
    /// After a later [`Dir::sync`] barrier on the same device, every file's
    /// data and attributes and every directory's entries and mode below are
    /// persisted — the explicit per-object ordering a staged tree needs
    /// before it may become authoritative, on any filesystem. A symlink or
    /// special entry is refused rather than followed.
    pub fn push_tree(&self) -> Result<()> {
        for name in self.list(None)? {
            let relative = PathBuf::from(&name);
            match self.entry_type(&relative)? {
                EntryType::Directory => self.subdir(&relative)?.push_tree()?,
                EntryType::File => push(&self.open_file(&relative)?)?,
                kind => {
                    return Err(invalid(format!(
                        "pushed tree holds an unsupported entry: {kind:?}"
                    )));
                }
            }
        }
        self.push()
    }
    /// Classify a final path entry without following it or any parent link.
    pub fn entry_type(&self, path: &Path) -> Result<EntryType> {
        let (parent, name) = self.parent(path, false)?;
        // SAFETY: the descriptor/name are live and fstatat does not retain them.
        let mut status = unsafe { std::mem::zeroed::<libc::stat>() };
        // SAFETY: query the final entry itself rather than following a link.
        if unsafe {
            libc::fstatat(
                parent.as_raw_fd(),
                name.as_ptr(),
                &mut status,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } < 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        let kind = status.st_mode & libc::S_IFMT;
        Ok(if kind == libc::S_IFDIR {
            EntryType::Directory
        } else if kind == libc::S_IFREG {
            EntryType::File
        } else if kind == libc::S_IFLNK {
            EntryType::Symlink
        } else {
            EntryType::Special
        })
    }
    /// Opens one real child directory through this descriptor, links never followed.
    ///
    /// `path` is relative to this directory and resolved component by component
    /// with `openat`/`O_NOFOLLOW`, so a substituted symlink anywhere on the way
    /// fails instead of resolving. The caller should re-verify identity with
    /// [`Dir::entry`] on the result when it must act on the same object it
    /// classified beforehand.
    pub fn subdir(&self, path: &Path) -> Result<Dir> {
        Ok(Dir(self.open_directory(path)?))
    }
    /// Restores owner write permission on this already-open directory descriptor.
    ///
    /// Retention of obsolete owner-read-only snapshots (directory `0500`, files
    /// `0400`) must unlink entries inside the snapshot before removing it. The
    /// `fchmod` runs on the live descriptor this process opened and verified,
    /// so it can never change permissions of anything outside that one judged
    /// directory, and never follows a path again.
    pub fn permit_owner_write(&self) -> Result<()> {
        self.0
            .set_permissions(std::fs::Permissions::from_mode(0o700))?;
        Ok(())
    }
    /// Drops owner write permission on this already-open directory descriptor.
    ///
    /// Publishing an immutable shared-store tree leaves every directory it
    /// contains at mode `0o500` (owner read and traverse only). Like
    /// [`Dir::permit_owner_write`] the permission change runs on the live
    /// descriptor this process opened and verified, so it restricts exactly
    /// that one directory and never resolves a path again.
    pub fn restrict_owner_read(&self) -> Result<()> {
        self.0
            .set_permissions(std::fs::Permissions::from_mode(0o500))?;
        Ok(())
    }
    /// Sets the permission bits of one owned final entry below this
    /// directory without following it or any parent link.
    ///
    /// `mode` is applied exactly (`fchmodat` with `AT_SYMLINK_NOFOLLOW`
    /// through the no-follow parent descriptor), so a symlink at `path` is
    /// never dereferenced and nothing outside this directory changes.
    pub fn set_mode(&self, path: &Path, mode: u32) -> Result<()> {
        let (parent, name) = self.parent(path, false)?;
        // SAFETY: parent is a live directory descriptor and name is NUL-terminated.
        if unsafe {
            libc::fchmodat(
                parent.as_raw_fd(),
                name.as_ptr(),
                mode as libc::mode_t,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } < 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }
    /// Read a final symbolic-link target without following it or its parents.
    pub fn read_link(&self, path: &Path) -> Result<Option<PathBuf>> {
        let (parent, name) = self.parent(path, false)?;
        let mut buffer = vec![0_u8; 4096];
        // SAFETY: the descriptor/name/buffer remain valid for this call.
        let count = unsafe {
            libc::readlinkat(
                parent.as_raw_fd(),
                name.as_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
            )
        };
        if count < 0 {
            let error = std::io::Error::last_os_error();
            return if error.kind() == std::io::ErrorKind::NotFound {
                Ok(None)
            } else {
                Err(error.into())
            };
        }
        if count as usize == buffer.len() {
            return Err(invalid("managed link target exceeds bound"));
        }
        buffer.truncate(count as usize);
        use std::os::unix::ffi::OsStringExt;
        Ok(Some(PathBuf::from(OsString::from_vec(buffer))))
    }
    /// Open a real child directory through no-follow descriptors.
    fn open_directory(&self, path: &Path) -> Result<File> {
        let (parent, name) = self.parent(path, false)?;
        // SAFETY: descriptor/name remain live for this system call.
        let raw = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if raw < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: this call owns the fresh descriptor.
        Ok(unsafe { File::from_raw_fd(raw) })
    }
    fn parent(&self, path: &Path, create: bool) -> Result<(File, CString)> {
        let parts = relative(path)?;
        let mut fd = self.0.try_clone()?;
        for name in &parts[..parts.len() - 1] {
            if create {
                // SAFETY: directory descriptor and NUL-terminated name remain live.
                let r = unsafe { libc::mkdirat(fd.as_raw_fd(), name.as_ptr(), 0o700) };
                if r < 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::EEXIST) {
                    return Err(std::io::Error::last_os_error().into());
                }
                if r == 0 {
                    // Python synchronizes every directory it creates before it
                    // descends (`_open_managed_parent`, adapters/home.py:163-168),
                    // so a crash cannot lose the directory entry that a
                    // published file lives in.
                    sync_directory(&fd)?;
                }
            }
            // SAFETY: openat only observes its valid arguments; returned fd is uniquely owned.
            let n = unsafe {
                libc::openat(
                    fd.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if n < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            // SAFETY: n is a newly allocated, nonnegative descriptor.
            fd = unsafe { File::from_raw_fd(n) };
        }
        Ok((fd, parts.last().expect("nonempty validated path").clone()))
    }
    /// Creates, or validates an existing, owned real directory at `path`
    /// (missing ancestors are created) and syncs its parent directory. An
    /// existing symlink or non-directory at `path` is refused.
    pub fn directory(&self, path: &Path) -> Result<()> {
        self.make_directory(path, Flush::Durable)
    }
    /// [`Dir::directory`] without the parent-directory durability flush, for
    /// a disposable staging tree built parent-first ([`Flush::Skipped`]);
    /// before the tree becomes live the caller pushes it
    /// ([`Dir::push_tree`]) and issues one [`Dir::sync`] barrier. Missing
    /// ancestors are still created (and synced) like [`Dir::write`].
    pub fn stage_directory(&self, path: &Path) -> Result<()> {
        self.make_directory(path, Flush::Skipped)
    }
    /// Creates, or validates an existing, owned real directory at `path`
    /// like [`Dir::directory`], flushing its parent directory to `level`.
    /// Missing ancestors are created and synced like [`Dir::write`].
    pub fn make_directory(&self, path: &Path, level: Flush) -> Result<()> {
        let (parent, name) = self.parent(path, true)?;
        // SAFETY: parent and name are live, mkdirat does not retain pointers.
        let n = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
        if n < 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::EEXIST) {
            return Err(std::io::Error::last_os_error().into());
        }
        // Validate an existing directory, rejecting links.
        // SAFETY: all pointers/descriptors are valid for the duration of this call.
        let n = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if n < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: n is newly allocated by openat.
        let _directory = unsafe { File::from_raw_fd(n) };
        flush(&parent, level)
    }
    pub fn open_file(&self, path: &Path) -> Result<File> {
        let (parent, name) = self.parent(path, false)?;
        // O_NONBLOCK prevents a substituted FIFO from hanging before the type check.
        // SAFETY: live descriptor and NUL-terminated path.
        let n = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if n < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: this code owns the new descriptor.
        let f = unsafe { File::from_raw_fd(n) };
        if !f.metadata()?.is_file() {
            return Err(Error::Integrity(
                "owned artifact must be a regular file".into(),
            ));
        }
        Ok(f)
    }
    pub fn read(&self, path: &Path, max: usize) -> Result<Vec<u8>> {
        let f = self.open_file(path)?;
        if f.metadata()?.len() > max as u64 {
            return Err(Error::Integrity("artifact exceeds read bound".into()));
        }
        let mut b = Vec::new();
        f.take(max as u64 + 1).read_to_end(&mut b)?;
        if b.len() > max {
            return Err(Error::Integrity("artifact exceeds read bound".into()));
        }
        Ok(b)
    }
    pub fn optional(&self, path: &Path, max: usize) -> Result<Option<Vec<u8>>> {
        match self.read(path, max) {
            Ok(b) => Ok(Some(b)),
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
    pub fn write(&self, path: &Path, data: &[u8], mode: u32) -> Result<()> {
        self.write_seamed(path, data, mode, None)
    }
    /// Same durable temp-then-rename write as [`Dir::write`], with an optional
    /// fault hook fired at the three crash points a real publish can be
    /// interrupted at. `fault` is always `None` on every production call
    /// site (`write` above); tests pass a hook to prove an interruption never
    /// exposes a partial file under `path`'s real name, mirroring
    /// `write_managed_file` (adapters/home.py:255-322).
    ///
    /// When `fault` fires, this simulates the process dying at that exact
    /// instruction: the normal temp-file cleanup below never runs, because a
    /// real crash there would not run it either. A non-simulated I/O error
    /// still runs cleanup, since the process is alive to attempt it.
    pub fn write_seamed(
        &self,
        path: &Path,
        data: &[u8],
        mode: u32,
        fault: Option<&dyn Fn(FaultPoint) -> Result<()>>,
    ) -> Result<()> {
        let fault_fired = std::cell::Cell::new(false);
        let fire = |point: FaultPoint| -> Result<()> {
            match fault {
                Some(f) => {
                    let outcome = f(point);
                    if outcome.is_err() {
                        fault_fired.set(true);
                    }
                    outcome
                }
                None => Ok(()),
            }
        };
        let (parent, name) = self.parent(path, true)?;
        let temporary = CString::new(format!(".agent-run-{}.tmp", uuid::Uuid::new_v4().simple()))
            .expect("ASCII UUID");
        // SAFETY: arguments live across call. O_EXCL ensures we own this new inode.
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                temporary.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                mode as libc::c_uint,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: unique fd returned by successful openat.
        let mut file = unsafe { File::from_raw_fd(fd) };
        let result = (|| -> Result<()> {
            file.write_all(data)?;
            // The real name is still untouched here: a crash leaves it absent
            // or at its previous content, never a partial write of `data`.
            fire(FaultPoint::MidWrite)?;
            file.sync_all()?;
            fire(FaultPoint::BeforeRename)?;
            // SAFETY: both names are descriptor-relative; rename replaces, never follows, a link.
            if unsafe {
                libc::renameat(
                    parent.as_raw_fd(),
                    temporary.as_ptr(),
                    parent.as_raw_fd(),
                    name.as_ptr(),
                )
            } < 0
            {
                return Err(std::io::Error::last_os_error().into());
            }
            // The rename already committed: `path` now holds the full new
            // bytes even if the parent-directory sync below never runs.
            fire(FaultPoint::AfterRename)?;
            sync_directory(&parent)?;
            Ok(())
        })();
        if result.is_err() && !fault_fired.get() {
            // SAFETY: removing only the fresh temporary name created by this invocation.
            unsafe { libc::unlinkat(parent.as_raw_fd(), temporary.as_ptr(), 0) };
        }
        result
    }
    pub fn symlink(&self, target: &Path, path: &Path) -> Result<()> {
        use std::os::unix::ffi::OsStrExt;
        let target = CString::new(target.as_os_str().as_bytes())
            .map_err(|_| invalid("invalid auth target"))?;
        let (parent, name) = self.parent(path, true)?;
        // SAFETY: NUL-terminated paths and a valid directory fd. Never overwrites an existing path.
        if unsafe { libc::symlinkat(target.as_ptr(), parent.as_raw_fd(), name.as_ptr()) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        sync_directory(&parent)?;
        Ok(())
    }
    /// Links one verified regular source file under a new name in this directory.
    ///
    /// `path` is the destination relative to this directory and `source_path`
    /// is the source relative to `source`; both are resolved component by
    /// component through no-follow descriptors, exactly like [`Dir::write`].
    /// Publication is no-replace: when `path` already exists the link is not
    /// created and the call returns `Ok(false)`, leaving the existing entry
    /// untouched; every other failure propagates. A first no-follow `fstatat`
    /// refuses non-regular sources — with `linkat` flags `0` a symbolic link
    /// would be linked as a link rather than followed — but that pre-stat is
    /// a filter, not a race guard: the source name could be substituted
    /// before the link lands. Publication is therefore only accepted after a
    /// second no-follow stat of the fresh destination proves it bound the
    /// exact `(device, inode)` observed beforehand; on any mismatch the new
    /// link is unlinked again and the call fails. Missing destination parent
    /// directories are created like [`Dir::write`], and the destination
    /// parent directory is synchronized after an accepted link.
    pub fn hardlink(&self, path: &Path, source: &Dir, source_path: &Path) -> Result<bool> {
        self.hardlink_flushed(path, source, source_path, Flush::Durable)
    }
    /// [`Dir::hardlink`] flushing the destination parent directory to
    /// `level` instead of always syncing it durably.
    pub fn hardlink_flushed(
        &self,
        path: &Path,
        source: &Dir,
        source_path: &Path,
        level: Flush,
    ) -> Result<bool> {
        let (source_parent, source_name) = source.parent(source_path, false)?;
        // SAFETY: stat is a plain C output buffer; fstatat initializes it before use.
        let mut status = unsafe { std::mem::zeroed::<libc::stat>() };
        // SAFETY: descriptor/name are live and fstatat retains neither.
        if unsafe {
            libc::fstatat(
                source_parent.as_raw_fd(),
                source_name.as_ptr(),
                &mut status,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } < 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        if status.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(invalid("hardlink source must be a regular file"));
        }
        let observed = (status.st_dev as u64, status.st_ino as u64);
        let (parent, name) = self.parent(path, true)?;
        // SAFETY: both descriptors and NUL-terminated names are live for this
        // call; flags 0 never dereference a symlink at the source name.
        if unsafe {
            libc::linkat(
                source_parent.as_raw_fd(),
                source_name.as_ptr(),
                parent.as_raw_fd(),
                name.as_ptr(),
                0,
            )
        } < 0
        {
            let error = std::io::Error::last_os_error();
            return if error.raw_os_error() == Some(libc::EEXIST) {
                Ok(false)
            } else {
                Err(error.into())
            };
        }
        // SAFETY: stat is a plain C output buffer; fstatat initializes it before use.
        let mut linked = unsafe { std::mem::zeroed::<libc::stat>() };
        // SAFETY: descriptor/name are live and fstatat retains neither; the
        // no-follow flag keeps a substituted destination name from being read
        // through.
        let identity = unsafe {
            libc::fstatat(
                parent.as_raw_fd(),
                name.as_ptr(),
                &mut linked,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if identity < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if linked.st_mode & libc::S_IFMT != libc::S_IFREG
            || (linked.st_dev as u64, linked.st_ino as u64) != observed
        {
            // SAFETY: unlink only the destination name this call just created.
            unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) };
            sync_directory(&parent)?;
            return Err(invalid("hardlink did not bind the verified source object"));
        }
        flush(&parent, level)?;
        Ok(true)
    }
    /// Atomically renames one entry beneath this directory without following
    /// links and without replacing anything already at the destination.
    ///
    /// `from` and `to` are relative to this directory and resolved through
    /// no-follow parent descriptors. The move uses the kernel's no-replace
    /// form (`renameatx_np` with `RENAME_EXCL` on macOS, `renameat2` with
    /// `RENAME_NOREPLACE` on Linux): when `to` already exists — including as
    /// an empty directory or a regular file — the kernel refuses the move,
    /// nothing is touched, and the call returns `Ok(false)`. Hosts without a
    /// no-replace rename fail with [`Error::Unsupported`] rather than falling
    /// back to a replaceable rename. Missing parent directories of `to` are
    /// created like [`Dir::write`], and both affected parent directories are
    /// synchronized after a move.
    pub fn rename_entry_no_replace(&self, from: &Path, to: &Path) -> Result<bool> {
        self.rename_entry_no_replace_flushed(from, to, Flush::Durable)
    }
    /// [`Dir::rename_entry_no_replace`] flushing both affected parent
    /// directories to `level` instead of always syncing them durably.
    pub fn rename_entry_no_replace_flushed(
        &self,
        from: &Path,
        to: &Path,
        level: Flush,
    ) -> Result<bool> {
        let (from_parent, from_name) = self.parent(from, false)?;
        let (to_parent, to_name) = self.parent(to, true)?;
        match exclusive_rename(&from_parent, &from_name, &to_parent, &to_name)? {
            ExclusiveRename::Moved => {}
            ExclusiveRename::DestinationExists => return Ok(false),
        }
        flush(&from_parent, level)?;
        if from_parent.as_raw_fd() != to_parent.as_raw_fd() {
            flush(&to_parent, level)?;
        }
        Ok(true)
    }
    /// Atomically renames one entry beneath this directory, replacing
    /// whatever entry already exists at the destination.
    ///
    /// `from` and `to` are relative to this directory and resolved through
    /// no-follow parent descriptors exactly like
    /// [`Dir::rename_entry_no_replace`], but the move uses the kernel's
    /// plain replacing `renameat`: an entry already at `to` — a regular
    /// file or a symbolic link, never dereferenced — is atomically replaced,
    /// so an outside observer sees either the old entry or the complete new
    /// one, never a missing name. It is the caller's job to prove the
    /// replaced name quiescent; the primitive itself serializes nothing.
    /// Missing parent directories of `to` are created like [`Dir::write`],
    /// and both affected parent directories are synchronized after the move.
    pub fn rename_entry_replace(&self, from: &Path, to: &Path) -> Result<()> {
        let (from_parent, from_name) = self.parent(from, false)?;
        let (to_parent, to_name) = self.parent(to, true)?;
        // SAFETY: live descriptors and NUL-terminated names; renameat
        // replaces the destination entry and never follows a link at either
        // name.
        if unsafe {
            libc::renameat(
                from_parent.as_raw_fd(),
                from_name.as_ptr(),
                to_parent.as_raw_fd(),
                to_name.as_ptr(),
            )
        } < 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        sync_directory(&from_parent)?;
        if from_parent.as_raw_fd() != to_parent.as_raw_fd() {
            sync_directory(&to_parent)?;
        }
        Ok(())
    }
    /// Creates one new exclusive file beneath this directory for streaming.
    ///
    /// `path` is relative to this directory and resolved through no-follow
    /// parent descriptors like [`Dir::write`]; missing parent directories are
    /// created. The file is created with `O_CREAT|O_EXCL` at `mode`, so the
    /// caller owns a brand-new inode no other name can already reference —
    /// the streaming-publish counterpart of [`Dir::write_seamed`]'s temp
    /// file. Unlike `write`, the returned [`File`] is unsynced and the parent
    /// is not synchronized; the caller streams bytes, syncs the file, and
    /// atomically renames it into place. A failure to create the name (for
    /// example an existing entry) propagates without touching anything.
    pub fn create_exclusive(&self, path: &Path, mode: u32) -> Result<File> {
        let (parent, name) = self.parent(path, true)?;
        // SAFETY: parent descriptor and NUL-terminated name remain live for
        // this call; O_EXCL guarantees the new inode has no other name.
        let raw = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                mode as libc::c_uint,
            )
        };
        if raw < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: this call owns the fresh descriptor returned by openat.
        Ok(unsafe { File::from_raw_fd(raw) })
    }
    /// Remove one owned entry through its no-follow parent. Unlink never
    /// dereferences the final component, so a symlink there is removed as
    /// the link itself, matching the descriptor-anchored read/write above.
    pub fn remove(&self, path: &Path) -> Result<()> {
        self.unlink(path, 0, Flush::Durable)
    }
}
/// Outcome of one kernel no-replace rename attempt between live descriptors.
enum ExclusiveRename {
    /// The move completed; nothing else changed.
    Moved,
    /// The destination already existed and the kernel refused the move.
    DestinationExists,
}

/// Performs the platform's exclusive rename between two live parent
/// descriptors, mapping `EEXIST` to [`ExclusiveRename::DestinationExists`];
/// hosts and kernels without a no-replace rename fail with
/// [`Error::Unsupported`], and every other failure propagates.
#[cfg(target_os = "macos")]
fn exclusive_rename(
    from_parent: &File,
    from_name: &CStr,
    to_parent: &File,
    to_name: &CStr,
) -> Result<ExclusiveRename> {
    // SAFETY: live descriptors and NUL-terminated names; RENAME_EXCL refuses
    // the move instead of ever replacing an existing destination entry.
    if unsafe {
        libc::renameatx_np(
            from_parent.as_raw_fd(),
            from_name.as_ptr(),
            to_parent.as_raw_fd(),
            to_name.as_ptr(),
            libc::RENAME_EXCL,
        )
    } < 0
    {
        return exclusive_rename_outcome();
    }
    Ok(ExclusiveRename::Moved)
}

/// Linux form of [`exclusive_rename`] using `renameat2` + `RENAME_NOREPLACE`.
#[cfg(target_os = "linux")]
fn exclusive_rename(
    from_parent: &File,
    from_name: &CStr,
    to_parent: &File,
    to_name: &CStr,
) -> Result<ExclusiveRename> {
    // SAFETY: live descriptors and NUL-terminated names; RENAME_NOREPLACE
    // refuses the move instead of ever replacing an existing destination.
    if unsafe {
        libc::renameat2(
            from_parent.as_raw_fd(),
            from_name.as_ptr(),
            to_parent.as_raw_fd(),
            to_name.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    } < 0
    {
        return exclusive_rename_outcome();
    }
    Ok(ExclusiveRename::Moved)
}

/// Other hosts have no no-replace rename this module is willing to use.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn exclusive_rename(
    _from_parent: &File,
    _from_name: &CStr,
    _to_parent: &File,
    _to_name: &CStr,
) -> Result<ExclusiveRename> {
    Err(Error::Unsupported(
        "host offers no no-replace rename for content-addressed publication".into(),
    ))
}

/// Classifies one failed exclusive rename: an existing destination is the
/// documented no-move outcome, an unavailable syscall is unsupported, and
/// anything else is a real error.
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn exclusive_rename_outcome() -> Result<ExclusiveRename> {
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EEXIST) => Ok(ExclusiveRename::DestinationExists),
        Some(libc::ENOSYS | libc::EINVAL | libc::ENOTSUP) => Err(Error::Unsupported(
            "host kernel offers no no-replace rename for content-addressed publication".into(),
        )),
        _ => Err(error.into()),
    }
}

pub fn canonical_json(value: &serde_json::Value) -> Result<Vec<u8>> {
    // Explicit recursion stays canonical even if another dependency enables
    // serde_json's preserve_order feature through Cargo feature unification.
    fn normalized(value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let mut keys: Vec<_> = map.keys().collect();
                keys.sort();
                serde_json::Value::Object(
                    keys.into_iter()
                        .map(|key| (key.clone(), normalized(&map[key])))
                        .collect(),
                )
            }
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.iter().map(normalized).collect())
            }
            value => value.clone(),
        }
    }
    Ok(serde_json::to_vec(&normalized(value))?)
}

#[cfg(test)]
/// Checks that bounded scans own independent offsets and converge beyond one batch.
mod scan_tests {
    use super::*;

    /// Two fresh scans repeat the first page; one live scan reaches every later entry.
    #[test]
    fn independent_offsets_and_multi_batch_convergence() {
        let home = tempfile::tempdir().unwrap();
        for index in 0..40 {
            std::fs::write(home.path().join(format!("file-{index:02}")), "x").unwrap();
        }
        let dir = Dir::open(home.path()).unwrap();
        let first = dir.list_batch(16).unwrap().0;
        assert_eq!(first, dir.list_batch(16).unwrap().0);
        let mut scan = dir.scan().unwrap();
        let mut seen = std::collections::BTreeSet::new();
        loop {
            let (names, done) = scan.next_batch(16).unwrap();
            seen.extend(names);
            if done {
                break;
            }
        }
        assert_eq!(seen.len(), 40);
    }
}
