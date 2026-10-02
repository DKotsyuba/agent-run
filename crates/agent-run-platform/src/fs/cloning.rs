//! Independent APFS clones for frozen assets and exact-byte idle cache reuse.
use super::Dir;
#[cfg(target_os = "macos")]
use super::sync_directory;
#[cfg(target_os = "macos")]
use agent_run_domain::Error;
use agent_run_domain::Result;
use std::path::Path;
#[cfg(target_os = "macos")]
use std::{
    ffi::CString,
    fs::File,
    io::Read,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::PermissionsExt,
    },
};

/// Checks whether cloning would preserve only the provenance xattr that macOS
/// already assigns to normally written managed files. Other source metadata
/// uses the byte writer so snapshot publication keeps its existing behavior.
#[cfg(target_os = "macos")]
fn clone_attributes_compatible(file: &File) -> bool {
    // SAFETY: fstat initializes this plain C output buffer for the live descriptor.
    let mut status = unsafe { std::mem::zeroed::<libc::stat>() };
    // SAFETY: the descriptor and output buffer remain valid for this call.
    if unsafe { libc::fstat(file.as_raw_fd(), &mut status) } < 0 || status.st_flags != 0 {
        return false;
    }
    // SAFETY: the descriptor is live; a null buffer asks only for the name length.
    let count = unsafe { libc::flistxattr(file.as_raw_fd(), std::ptr::null_mut(), 0, 0) };
    if !(0..=256).contains(&count) {
        return false;
    }
    if count == 0 {
        return true;
    }
    let mut names = vec![0_u8; count as usize];
    // SAFETY: the allocated buffer remains live and has the queried length.
    let written =
        unsafe { libc::flistxattr(file.as_raw_fd(), names.as_mut_ptr().cast(), names.len(), 0) };
    written == count
        && names.last() == Some(&0)
        && names[..names.len() - 1]
            .split(|byte| *byte == 0)
            .all(|name| name == b"com.apple.provenance")
}

impl Dir {
    /// Publishes captured snapshot bytes, cloning their source blocks when macOS supports it.
    ///
    /// `source_path` and `path` are no-follow relative paths in their respective
    /// directories. The returned boolean reports a verified APFS clone; false
    /// means the existing byte writer published `data`. Unsupported volumes,
    /// changed source bytes, and source xattrs other than system provenance
    /// fall back to that writer. Other clone failures propagate without
    /// replacing the destination. Both paths publish through a temporary name
    /// and retain the same private mode and parent-directory sync contract.
    pub fn write_snapshot_file(
        &self,
        path: &Path,
        source: &Dir,
        source_path: &Path,
        data: &[u8],
        mode: u32,
    ) -> Result<bool> {
        #[cfg(target_os = "macos")]
        if self.try_clone_snapshot(path, source, source_path, data, mode, None, true)? {
            return Ok(true);
        }
        #[cfg(not(target_os = "macos"))]
        let _ = (source, source_path);
        self.write(path, data, mode)?;
        Ok(false)
    }

    /// [`Dir::write_snapshot_file`] for a disposable staging tree: the same
    /// verified exact-byte clone and private mode, without the per-file and
    /// parent-directory durability flushes.
    ///
    /// Nothing is flushed: before the staged tree becomes live the caller
    /// must push every staged object ([`Dir::push_tree`]) and then issue one
    /// [`Dir::sync`] barrier, and its recovery must prove or discard a staged
    /// tree a crash interrupted. Unsupported volumes fall back to the synced
    /// byte writer. Returns whether a clone was published.
    pub fn stage_snapshot_file(
        &self,
        path: &Path,
        source: &Dir,
        source_path: &Path,
        data: &[u8],
        mode: u32,
    ) -> Result<bool> {
        #[cfg(target_os = "macos")]
        if self.try_clone_snapshot(path, source, source_path, data, mode, None, false)? {
            return Ok(true);
        }
        #[cfg(not(target_os = "macos"))]
        let _ = (source, source_path);
        self.write(path, data, mode)?;
        Ok(false)
    }

    /// Clones one whole directory hierarchy into a new, unsynced name.
    ///
    /// `source_path` names a real directory below `source` and `path` a new
    /// name below this directory; both are resolved through no-follow parent
    /// descriptors, and the kernel clone itself follows no link anywhere in
    /// the hierarchy. On APFS one `clonefileat` gives every entry an
    /// independent inode sharing the source's data blocks, with the source's
    /// modes and extended attributes — callers normalize modes afterwards,
    /// then push every cloned object ([`Dir::push_tree`]) and issue one
    /// [`Dir::sync`] barrier before the tree becomes live.
    /// Returns `false` without creating anything when the volume or host
    /// cannot clone directories (non-macOS, non-APFS, cross-device), so the
    /// caller falls back to per-entry staging; other failures propagate and
    /// leave at most a partial clone at `path` for the caller's recovery.
    pub fn clone_directory(&self, path: &Path, source: &Dir, source_path: &Path) -> Result<bool> {
        #[cfg(target_os = "macos")]
        {
            let (source_parent, source_name) = source.parent(source_path, false)?;
            let (parent, name) = self.parent(path, true)?;
            // SAFETY: both parents are live directory descriptors and both
            // names NUL-terminated; the flags refuse every symlink and never
            // copy foreign ownership.
            if unsafe {
                libc::clonefileat(
                    source_parent.as_raw_fd(),
                    source_name.as_ptr(),
                    parent.as_raw_fd(),
                    name.as_ptr(),
                    0x0008 | 0x0002,
                )
            } < 0
            {
                let error = std::io::Error::last_os_error();
                return if matches!(
                    error.raw_os_error(),
                    Some(libc::ENOTSUP | libc::EXDEV | libc::ENOSYS)
                ) {
                    Ok(false)
                } else {
                    Err(error.into())
                };
            }
            Ok(true)
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (path, source, source_path);
            Ok(false)
        }
    }

    /// Replaces an idle cache file with an independent clone only when bytes match.
    ///
    /// Both paths are relative, no-follow file paths. The caller must own an
    /// idle destination; its bytes, mode, ACL, and timestamps are preserved.
    /// Files larger than `max_bytes`, unsupported metadata/filesystems, and
    /// non-macOS hosts return zero without rewriting the destination. A positive
    /// result is the logical byte count now referencing cloned blocks, not a
    /// measurement of physical space reclaimed. I/O failures propagate.
    pub fn clone_matching_file(
        &self,
        path: &Path,
        source: &Dir,
        source_path: &Path,
        max_bytes: usize,
    ) -> Result<usize> {
        #[cfg(target_os = "macos")]
        {
            let mut original = self.open_file(path)?;
            let metadata = original.metadata()?;
            if metadata.len() == 0
                || metadata.len() > max_bytes as u64
                || !clone_attributes_compatible(&original)
            {
                return Ok(0);
            }
            let mut data = Vec::new();
            Read::by_ref(&mut original)
                .take(max_bytes.saturating_add(1) as u64)
                .read_to_end(&mut data)?;
            if data.len() > max_bytes || data.len() as u64 != metadata.len() {
                return Ok(0);
            }
            let cloned = self.try_clone_snapshot(
                path,
                source,
                source_path,
                &data,
                metadata.permissions().mode() & 0o777,
                Some(&original),
                false,
            )?;
            Ok(if cloned { data.len() } else { 0 })
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (path, source, source_path, max_bytes);
            Ok(0)
        }
    }

    /// Returns true only after an exact-byte clone has been atomically renamed.
    /// Optional `metadata_source` preserves an existing idle cache file's
    /// metadata before publication; snapshots use their requested private mode.
    /// `durable` syncs the file and its parent; reconstructible caches and
    /// disposable staging trees omit those flushes.
    #[cfg(target_os = "macos")]
    #[allow(clippy::too_many_arguments)]
    fn try_clone_snapshot(
        &self,
        path: &Path,
        source: &Dir,
        source_path: &Path,
        data: &[u8],
        mode: u32,
        metadata_source: Option<&File>,
        durable: bool,
    ) -> Result<bool> {
        let source_file = match source.open_file(source_path) {
            Ok(file) => file,
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        if source_file.metadata()?.len() != data.len() as u64
            || !clone_attributes_compatible(&source_file)
        {
            return Ok(false);
        }
        let (parent, name) = self.parent(path, true)?;
        let temporary = CString::new(format!(".agent-run-{}.tmp", uuid::Uuid::new_v4().simple()))
            .expect("ASCII UUID");
        // SAFETY: source is an opened regular file; parent and temp basename
        // are owned and live. Flags prohibit path links and foreign ownership.
        if unsafe {
            libc::fclonefileat(
                source_file.as_raw_fd(),
                parent.as_raw_fd(),
                temporary.as_ptr(),
                0x0008 | 0x0002,
            )
        } < 0
        {
            let error = std::io::Error::last_os_error();
            return if matches!(
                error.raw_os_error(),
                Some(libc::ENOTSUP | libc::EXDEV | libc::ENOSYS | libc::EINVAL)
            ) {
                Ok(false)
            } else {
                Err(error.into())
            };
        }
        let result = (|| -> Result<bool> {
            // SAFETY: clonefileat just created this unique temp name in parent.
            let fd = unsafe {
                libc::openat(
                    parent.as_raw_fd(),
                    temporary.as_ptr(),
                    libc::O_RDONLY | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if fd < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            // SAFETY: this invocation uniquely owns the opened descriptor.
            let mut file = unsafe { File::from_raw_fd(fd) };
            if !file.metadata()?.is_file()
                || file.metadata()?.len() != data.len() as u64
                || !clone_attributes_compatible(&file)
            {
                return Ok(false);
            }
            let mut captured = Vec::with_capacity(data.len());
            Read::by_ref(&mut file)
                .take(data.len() as u64 + 1)
                .read_to_end(&mut captured)?;
            if captured != data {
                return Ok(false);
            }
            file.set_permissions(std::fs::Permissions::from_mode(mode))?;
            if let Some(original) = metadata_source {
                // SAFETY: both descriptors are live regular files; copy only
                // original destination metadata, never its data blocks.
                if unsafe {
                    libc::fcopyfile(
                        original.as_raw_fd(),
                        file.as_raw_fd(),
                        std::ptr::null_mut(),
                        libc::COPYFILE_STAT | libc::COPYFILE_ACL | libc::COPYFILE_XATTR,
                    )
                } < 0
                {
                    return Err(std::io::Error::last_os_error().into());
                }
            }
            if durable {
                file.sync_all()?;
            }
            // SAFETY: both names are relative to the verified directory;
            // rename replaces the destination atomically without following it.
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
            if durable {
                sync_directory(&parent)?;
            }
            Ok(true)
        })();
        if !matches!(result, Ok(true)) {
            // SAFETY: only the fresh temporary name owned by this invocation.
            unsafe { libc::unlinkat(parent.as_raw_fd(), temporary.as_ptr(), 0) };
        }
        result
    }
}

#[cfg(test)]
/// Verifies isolated snapshot bytes and macOS clone fallback.
mod snapshot_clone_tests {
    use super::*;
    use crate::fs::private_dir;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    /// Independent inodes keep their pinned bytes when either side changes.
    #[test]
    fn clone_keeps_private_snapshot_bytes_and_modes() {
        let temporary = tempfile::tempdir().unwrap();
        let source_path = temporary.path().join("source");
        let target_path = temporary.path().join("target");
        private_dir(&source_path).unwrap();
        private_dir(&target_path).unwrap();
        let source = Dir::open(&source_path).unwrap();
        let target = Dir::open(&target_path).unwrap();
        let old = b"frozen selected plugin version";
        source.write(Path::new("skill.txt"), old, 0o600).unwrap();
        let cloned = target
            .write_snapshot_file(
                Path::new("selected/skill.txt"),
                &source,
                Path::new("skill.txt"),
                old,
                0o700,
            )
            .unwrap();
        #[cfg(target_os = "macos")]
        assert!(cloned, "APFS temp homes should support native cloning");
        #[cfg(not(target_os = "macos"))]
        assert!(!cloned, "other targets use the existing byte writer");
        assert_eq!(
            target.read(Path::new("selected/skill.txt"), 1024).unwrap(),
            old
        );
        let stored = std::fs::metadata(target_path.join("selected/skill.txt")).unwrap();
        assert_eq!(stored.permissions().mode() & 0o777, 0o700);
        assert_ne!(
            stored.ino(),
            std::fs::metadata(source_path.join("skill.txt"))
                .unwrap()
                .ino()
        );
        source
            .write(Path::new("skill.txt"), b"new plugin version", 0o600)
            .unwrap();
        assert_eq!(
            target.read(Path::new("selected/skill.txt"), 1024).unwrap(),
            old
        );
        target
            .write(
                Path::new("selected/skill.txt"),
                b"per-run private change",
                0o700,
            )
            .unwrap();
        assert_eq!(
            source.read(Path::new("skill.txt"), 1024).unwrap(),
            b"new plugin version"
        );
    }

    /// Unselected source xattrs retain the historical byte-write behavior.
    #[cfg(target_os = "macos")]
    #[test]
    fn extra_source_xattr_falls_back_without_copying_metadata() {
        let temporary = tempfile::tempdir().unwrap();
        let source_path = temporary.path().join("source");
        let target_path = temporary.path().join("target");
        private_dir(&source_path).unwrap();
        private_dir(&target_path).unwrap();
        let source = Dir::open(&source_path).unwrap();
        let target = Dir::open(&target_path).unwrap();
        let payload = b"selected source";
        source.write(Path::new("asset"), payload, 0o600).unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(source_path.join("asset"))
            .unwrap();
        let name = c"com.agent_run_snapshot_test";
        let value = b"private xattr";
        assert_eq!(
            // SAFETY: name, value and owned descriptor stay valid for this syscall.
            unsafe {
                libc::fsetxattr(
                    file.as_raw_fd(),
                    name.as_ptr(),
                    value.as_ptr().cast(),
                    value.len(),
                    0,
                    0,
                )
            },
            0
        );
        assert!(
            !target
                .write_snapshot_file(
                    Path::new("asset"),
                    &source,
                    Path::new("asset"),
                    payload,
                    0o600
                )
                .unwrap()
        );
        assert_eq!(target.read(Path::new("asset"), 1024).unwrap(), payload);
        assert_eq!(
            // SAFETY: a null output buffer only queries whether the known xattr exists.
            unsafe {
                libc::fgetxattr(
                    target.open_file(Path::new("asset")).unwrap().as_raw_fd(),
                    name.as_ptr(),
                    std::ptr::null_mut(),
                    0,
                    0,
                    0,
                )
            },
            -1
        );
    }

    /// Source file flags are not inherited by a new managed snapshot file.
    #[cfg(target_os = "macos")]
    #[test]
    fn source_flags_fall_back_to_byte_writer() {
        let temporary = tempfile::tempdir().unwrap();
        let source_path = temporary.path().join("source");
        let target_path = temporary.path().join("target");
        private_dir(&source_path).unwrap();
        private_dir(&target_path).unwrap();
        let source = Dir::open(&source_path).unwrap();
        let target = Dir::open(&target_path).unwrap();
        let payload = b"selected source";
        source.write(Path::new("asset"), payload, 0o600).unwrap();
        let file = source.open_file(Path::new("asset")).unwrap();
        assert_eq!(
            // SAFETY: the descriptor is live and the user owns this temporary file.
            unsafe { libc::fchflags(file.as_raw_fd(), libc::UF_NODUMP) },
            0
        );
        assert!(
            !target
                .write_snapshot_file(
                    Path::new("asset"),
                    &source,
                    Path::new("asset"),
                    payload,
                    0o600
                )
                .unwrap()
        );
        assert_eq!(target.read(Path::new("asset"), 1024).unwrap(), payload);
        assert_eq!(
            std::fs::metadata(target_path.join("asset"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    /// Matching cache files share blocks while retaining destination metadata and isolation.
    #[cfg(target_os = "macos")]
    #[test]
    fn matching_cache_clone_preserves_metadata_and_independence() {
        let temporary = tempfile::tempdir().unwrap();
        let source_path = temporary.path().join("source");
        let target_path = temporary.path().join("target");
        private_dir(&source_path).unwrap();
        private_dir(&target_path).unwrap();
        let source = Dir::open(&source_path).unwrap();
        let target = Dir::open(&target_path).unwrap();
        let payload = vec![7_u8; 8192];
        source.write(Path::new("asset"), &payload, 0o600).unwrap();
        target.write(Path::new("asset"), &payload, 0o700).unwrap();
        let modified = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1234);
        target
            .open_file(Path::new("asset"))
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .unwrap();
        let old_inode = std::fs::metadata(target_path.join("asset")).unwrap().ino();
        assert_eq!(
            target
                .clone_matching_file(Path::new("asset"), &source, Path::new("asset"), 16384)
                .unwrap(),
            payload.len()
        );
        let metadata = std::fs::metadata(target_path.join("asset")).unwrap();
        assert_ne!(metadata.ino(), old_inode);
        assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
        assert_eq!(metadata.modified().unwrap(), modified);
        std::fs::write(source_path.join("asset"), vec![8_u8; 8192]).unwrap();
        assert_eq!(target.read(Path::new("asset"), 16384).unwrap(), payload);
        let retained_inode = metadata.ino();
        assert_eq!(
            target
                .clone_matching_file(Path::new("asset"), &source, Path::new("asset"), 16384)
                .unwrap(),
            0
        );
        assert_eq!(
            std::fs::metadata(target_path.join("asset")).unwrap().ino(),
            retained_inode
        );
        std::fs::remove_dir_all(source_path).unwrap();
        assert_eq!(target.read(Path::new("asset"), 16384).unwrap(), payload);
    }
}
