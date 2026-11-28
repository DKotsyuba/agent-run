//! Durable filesystem evidence for external, per-agent temp storage.

use crate::{Error, Result};
use agent_run_domain::{domain::AgentId, error::invalid};
use agent_run_platform::fs::{Dir, Entry, EntryType, TempAuthority, TrustedGroup};
use serde::{Deserialize, Serialize};
use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};

pub(crate) const RECORD: &str = "worker-temp.json";

/// Agent-store creation intent, completed by recording the verified device and inode.
#[derive(Serialize, Deserialize)]
pub(crate) struct Record {
    pub(crate) path: PathBuf,
    uid: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    device: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inode: Option<u64>,
    #[serde(default)]
    trusted: Vec<String>,
    #[serde(default)]
    groups: Vec<TrustedGroup>,
}

/// Reads only a private, worker-owned record. Absence permits legacy retention.
pub(crate) fn recorded(agent: &Dir) -> Result<Option<Record>> {
    match agent.read_private(Path::new(RECORD), 16 * 1024) {
        Ok(bytes) => {
            let record: Record = serde_json::from_slice(&bytes)?;
            if record.device.is_some() != record.inode.is_some() {
                return Err(invalid(
                    "worker temp record has incomplete directory identity",
                ));
            }
            Ok(Some(record))
        }
        Err(Error::Io(error)) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Opens an operator-provisioned root after checking its rename authority.
fn root(path: &Path, authority: &TempAuthority) -> Result<Dir> {
    let dir = Dir::open_worker_temp_root(path, authority)?;
    if !dir.has_access(libc::W_OK | libc::X_OK) {
        return Err(invalid(
            "worker_temp.root needs write and search access for the worker UID",
        ));
    }
    Ok(dir)
}

/// Creates a new leaf, repairs a missing one, or reuses its recorded identity.
pub(crate) fn prepare(
    root_path: &Path,
    project: &Path,
    id: &AgentId,
    agent: &Dir,
    trusted: &[String],
    groups: &[TrustedGroup],
) -> Result<PathBuf> {
    let root = root(root_path, &TempAuthority::new(trusted, groups)?)?;
    prepare_at(&root, root_path, project, id, agent, trusted, groups)
}

/// Prepares a leaf relative to an already validated root descriptor.
fn prepare_at(
    root: &Dir,
    root_path: &Path,
    project: &Path,
    id: &AgentId,
    agent: &Dir,
    trusted: &[String],
    groups: &[TrustedGroup],
) -> Result<PathBuf> {
    let name = Path::new(id.as_str());
    let path = root_path.join(project).join(name);
    let previous = recorded(agent)?;
    if previous.as_ref().is_some_and(|record| record.path != path) {
        return Err(invalid("worker_temp path differs from the recorded path"));
    }
    let parent = mkdir_or_open(root, project, 0o750)?;
    let authority = TempAuthority::new(trusted, groups)?;
    parent.check_worker_temp_access(&authority, false)?;
    // SAFETY: geteuid only reads process credentials.
    let uid = unsafe { libc::geteuid() };
    let mut record = Record {
        path,
        uid,
        device: None,
        inode: None,
        trusted: trusted.to_vec(),
        groups: groups.to_vec(),
    };
    let leaf = match parent.subdir(name) {
        Err(Error::Io(error)) if error.kind() == ErrorKind::NotFound => {
            record.create_leaf(agent, &parent, name)?
        }
        Ok(leaf) if previous.is_some() => {
            let previous = previous.as_ref().expect("checked intent");
            if !previous.matches(&leaf.entry(None)?) {
                return Err(invalid(
                    "worker temp directory does not match its recorded identity",
                ));
            }
            leaf
        }
        Ok(_) => {
            return Err(invalid(
                "worker temp leaf already exists without a creation intent",
            ));
        }
        Err(error) => return Err(error),
    };
    let entry = check_leaf(&leaf, &authority)?;
    leaf.check_worker_temp_mount(parent.entry(None)?.device)?;
    record.device = Some(entry.device);
    record.inode = Some(entry.inode);
    record.save(agent)?;
    Ok(record.path)
}

/// Creates one physical child, or opens a child left by an earlier attempt.
fn mkdir_or_open(parent: &Dir, name: &Path, mode: u32) -> Result<Dir> {
    match parent.create_directory_exclusive(name, mode) {
        Err(Error::Io(error)) if error.kind() == ErrorKind::AlreadyExists => parent.subdir(name),
        result => result,
    }
}

/// A leaf is worker-owned and grants the worker effective read, write and search.
fn check_leaf(leaf: &Dir, authority: &TempAuthority) -> Result<Entry> {
    let entry = leaf.entry(None)?;
    // SAFETY: geteuid only reads process credentials.
    if entry.uid != unsafe { libc::geteuid() }
        || !leaf.has_access(libc::R_OK | libc::W_OK | libc::X_OK)
    {
        return Err(invalid(
            "worker temp leaf must be worker-owned and grant the worker read, write and search access",
        ));
    }
    leaf.check_worker_temp_access(authority, false)?;
    Ok(entry)
}

/// Durable record location and the exact temp path handed to the harness.
pub(crate) struct LaunchGrant<'a> {
    pub(crate) agent: &'a Path,
    pub(crate) id: &'a AgentId,
    pub(crate) path: &'a Path,
}

impl LaunchGrant<'_> {
    /// Revalidates the leaf and its ancestors just before native handoff.
    pub(crate) fn verify(&self) -> Result<()> {
        let agent = Dir::open_searchable(self.agent)?;
        let record = recorded(&agent)?
            .filter(|record| record.path == self.path && record.device.is_some())
            .ok_or_else(|| invalid("worker temp launch record is missing or changed"))?;
        let Some((parent, leaf)) = record.open_leaf(self.id, false)? else {
            return Err(invalid("worker temp leaf disappeared before launch"));
        };
        check_leaf(&leaf, &TempAuthority::new(&record.trusted, &record.groups)?)?;
        leaf.check_worker_temp_mount(parent.entry(None)?.device)
    }
}

impl Record {
    /// Reopens a recorded directory through no-follow parents and verifies its identity.
    pub(crate) fn open_leaf(&self, id: &AgentId, retiring: bool) -> Result<Option<(Dir, Dir)>> {
        let name = Path::new(id.as_str());
        let parent = self
            .path
            .parent()
            .filter(|_| self.path.file_name() == Some(name.as_os_str()))
            .ok_or_else(|| invalid("invalid worker temp record"))?;
        let root_path = parent
            .parent()
            .ok_or_else(|| invalid("invalid worker temp project"))?;
        let project = parent
            .file_name()
            .ok_or_else(|| invalid("invalid worker temp project"))?;
        let authority = TempAuthority::new(&self.trusted, &self.groups)?;
        let parent =
            match root(root_path, &authority).and_then(|root| root.subdir(Path::new(project))) {
                Err(Error::Io(error)) if error.kind() == ErrorKind::NotFound => return Ok(None),
                result => result?,
            };
        parent.check_worker_temp_access(&authority, false)?;
        let expected = match parent.entry(Some(name)) {
            Err(Error::Io(error)) if error.kind() == ErrorKind::NotFound => return Ok(None),
            result => result?,
        };
        // SAFETY: geteuid only reads process credentials.
        if self.uid != unsafe { libc::geteuid() } || !self.matches(&expected) {
            return Err(invalid(
                "worker temp directory does not match its recorded identity",
            ));
        }
        let leaf = if retiring {
            parent.open_worker_temp_for_retention(name, &expected, parent.entry(None)?.device)?
        } else {
            parent.subdir(name)?
        };
        let observed = leaf.entry(None)?;
        if !self.matches(&observed)
            || (expected.device, expected.inode) != (observed.device, observed.inode)
        {
            return Err(invalid(
                "worker temp directory does not match its recorded identity",
            ));
        }
        leaf.check_worker_temp_access(&authority, false)?;
        Ok(Some((parent, leaf)))
    }

    /// Publishes intent before mkdir and withdraws it for a definite name collision.
    fn create_leaf(&self, agent: &Dir, parent: &Dir, name: &Path) -> Result<Dir> {
        self.save(agent)?;
        match parent.create_directory_exclusive(name, 0o770) {
            Err(Error::Io(error)) if error.kind() == ErrorKind::AlreadyExists => {
                agent.remove(Path::new(RECORD))?;
                Err(error.into())
            }
            result => result,
        }
    }

    /// Flushes the intent or completed identity under the private agent directory.
    fn save(&self, agent: &Dir) -> Result<()> {
        agent.write(Path::new(RECORD), &serde_json::to_vec(self)?, 0o600)
    }

    /// Checks a no-follow entry against the completed identity or a pending intent.
    pub(crate) fn matches(&self, entry: &Entry) -> bool {
        entry.kind == EntryType::Directory
            && entry.uid == self.uid
            && match (self.device, self.inode) {
                (Some(device), Some(inode)) => (entry.device, entry.inode) == (device, inode),
                (None, None) => true,
                _ => false,
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    /// Refuses symlinks, adopts a pre-record owned leaf and recreates a missing one.
    #[test]
    fn worker_temp_adopts_and_recreates_only_safe_leaves() {
        let fixture = tempfile::tempdir().unwrap();
        let path = fixture.path();
        let root = Dir::open(path).unwrap();
        let project = Path::new("project");
        let id = AgentId::new();
        let project_path = path.join(project);
        let leaf = project_path.join(id.as_str());
        let prepare = || prepare_at(&root, path, project, &id, &root, &[], &[]);
        symlink(path, &project_path).unwrap();
        assert!(prepare().is_err());
        std::fs::remove_file(&project_path).unwrap();
        root.create_directory_exclusive(project, 0o700).unwrap();
        symlink(path, &leaf).unwrap();
        assert!(prepare().is_err());
        std::fs::remove_file(&leaf).unwrap();
        #[cfg(target_os = "linux")]
        {
            use std::{ffi::CString, os::unix::ffi::OsStrExt};
            // A read-only inherited group mode must survive mkdir(0770).
            let project = CString::new(project_path.as_os_str().as_bytes()).unwrap();
            let acl = [
                2u8, 0, 0, 0, // POSIX ACL version.
                1, 0, 7, 0, 255, 255, 255, 255, // Owner rwx.
                4, 0, 5, 0, 255, 255, 255, 255, // Owning group r-x.
                32, 0, 0, 0, 255, 255, 255, 255, // Other ---.
            ];
            // SAFETY: the name and ACL bytes are live; the path is test-owned.
            let installed = unsafe {
                libc::setxattr(
                    project.as_ptr(),
                    c"system.posix_acl_default".as_ptr(),
                    acl.as_ptr().cast(),
                    acl.len(),
                    0,
                )
            };
            assert_eq!(installed, 0, "{}", std::io::Error::last_os_error());
        }
        // A failed durable write must leave the requested leaf absent.
        // SAFETY: geteuid only reads process credentials.
        if unsafe { libc::geteuid() } != 0 {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o500)).unwrap();
            assert!(prepare().is_err());
            assert!(!leaf.exists());
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let parent = root.subdir(project).unwrap();
        let original = parent
            .create_directory_exclusive(Path::new(id.as_str()), 0o700)
            .unwrap();
        assert!(prepare().is_err(), "an unrecorded leaf is never adopted");
        assert!(recorded(&root).unwrap().is_none());
        Record {
            path: leaf.clone(),
            uid: original.entry(None).unwrap().uid,
            device: None,
            inode: None,
            trusted: vec![],
            groups: vec![],
        }
        .save(&root)
        .unwrap();
        assert_eq!(prepare().unwrap(), leaf);
        let saved = recorded(&root).unwrap().unwrap();
        assert!(saved.matches(&original.entry(None).unwrap()));
        assert_eq!(
            std::fs::metadata(&leaf).unwrap().permissions().mode() & 0o777,
            0o700
        );
        std::fs::rename(&leaf, project_path.join("old")).unwrap();
        assert_eq!(prepare().unwrap(), leaf);
        #[cfg(target_os = "linux")]
        assert_eq!(
            std::fs::metadata(&leaf).unwrap().permissions().mode() & 0o777,
            0o750
        );
        let current = recorded(&root).unwrap().unwrap();
        assert!(!current.matches(&original.entry(None).unwrap()));
        std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o500)).unwrap();
        // Root can bypass mode checks; regular worker accounts cannot.
        // SAFETY: geteuid only reads process credentials.
        if unsafe { libc::geteuid() } != 0 {
            assert!(prepare().is_err());
        }
        std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::rename(&leaf, project_path.join("new")).unwrap();
        std::fs::rename(project_path.join("old"), &leaf).unwrap();
        assert!(prepare().is_err());
    }

    /// An actual exclusive mkdir collision removes the intent retention would consume.
    #[test]
    fn worker_temp_collision_withdraws_retention_intent() {
        let home = tempfile::tempdir().unwrap();
        let mut store = agent_run_store::fixture::initialize(home.path()).unwrap();
        let id = AgentId::new();
        let root = Dir::open(home.path()).unwrap();
        root.directory(Path::new("agents")).unwrap();
        let agents = root.subdir(Path::new("agents")).unwrap();
        let name = Path::new(id.as_str());
        let agent = agents.create_directory_exclusive(name, 0o700).unwrap();
        let parent = root
            .create_directory_exclusive(Path::new("project"), 0o700)
            .unwrap();
        let leaf = parent.create_directory_exclusive(name, 0o700).unwrap();
        leaf.write(Path::new("keep"), b"unrelated", 0o600).unwrap();
        let intent = Record {
            path: home.path().join("project").join(name),
            uid: leaf.entry(None).unwrap().uid,
            device: None,
            inode: None,
            trusted: vec![],
            groups: vec![],
        };
        let error = intent
            .create_leaf(&agent, &parent, name)
            .err()
            .expect("exclusive mkdir collision");
        assert!(matches!(error, Error::Io(error) if error.kind() == ErrorKind::AlreadyExists));
        assert!(recorded(&agent).unwrap().is_none());
        crate::housekeeping::sweep_account_worker_temps(
            &home.path().canonicalize().unwrap(),
            &mut store,
            &[],
        )
        .unwrap();
        assert_eq!(leaf.read(Path::new("keep"), 128).unwrap(), b"unrelated");
        assert!(parent.subdir(name).is_ok());
    }

    /// Nontrusted writers and owners are refused before creation.
    #[test]
    fn worker_temp_checks_rename_authority() {
        let temp = tempfile::tempdir().unwrap();
        let dir = Dir::open(temp.path()).unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o702)).unwrap();
        let authority = TempAuthority::new(&[], &[]).unwrap();
        assert!(dir.check_worker_temp_access(&authority, false).is_err());
        let foreign = Dir::open(Path::new("/")).unwrap();
        let uid = foreign.entry(None).unwrap().uid;
        // SAFETY: geteuid only reads process credentials.
        if uid != unsafe { libc::geteuid() } {
            assert!(foreign.check_worker_temp_access(&authority, false).is_err());
        }
    }
}
