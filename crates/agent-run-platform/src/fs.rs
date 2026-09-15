//! Descriptor-anchored local storage. No symlinks are followed inside an owned tree.
use agent_run_domain::{error::invalid, Error, Result};
use sha2::{Digest, Sha256};
use std::{
    ffi::CString,
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{OpenOptionsExt, PermissionsExt},
    },
    path::{Component, Path, PathBuf},
};

pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
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
pub struct Dir(File);
impl Dir {
    pub fn open(path: &Path) -> Result<Self> {
        let f = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
            .open(path)?;
        Ok(Self(f))
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
        parent.sync_all()?;
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
            file.sync_all()?;
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
            parent.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
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
        parent.sync_all()?;
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
