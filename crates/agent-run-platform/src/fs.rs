//! Descriptor-anchored local storage. No symlinks are followed inside an owned tree.
use agent_run_domain::{error::invalid, Error, Result};
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
    if let Ok(m) = std::fs::symlink_metadata(path) {
        if m.file_type().is_symlink() || !m.is_dir() {
            return Err(invalid("private home must be a real directory"));
        }
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
/// [`tolerate_unsupported_sync`] for the exact tolerated errors.
fn sync_directory(directory: &File) -> Result<()> {
    tolerate_unsupported_sync(directory.sync_all())
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
pub struct Dir(File);
impl Dir {
    pub fn open(path: &Path) -> Result<Self> {
        let f = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
            .open(path)?;
        Ok(Self(f))
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
    pub fn directory(&self, path: &Path) -> Result<()> {
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
        sync_directory(&parent)?;
        Ok(())
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
    /// Remove one owned entry through its no-follow parent. Unlink never
    /// dereferences the final component, so a symlink there is removed as
    /// the link itself, matching the descriptor-anchored read/write above.
    pub fn remove(&self, path: &Path) -> Result<()> {
        let (parent, name) = self.parent(path, false)?;
        // SAFETY: parent is a live directory descriptor and name is NUL-terminated.
        if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        sync_directory(&parent)?;
        Ok(())
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
