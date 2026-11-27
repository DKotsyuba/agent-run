//! Worker-identity access checks for provider admission and actual launch.

use crate::{Result, profiles::Profile};
use agent_run_config::provider_config::ProviderConfig;
use agent_run_domain::{
    ProviderStartRequest, catalog::HarnessId, domain::StartRequest, error::invalid,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    ffi::CString,
    fs,
    io::Read,
    os::unix::{
        ffi::OsStrExt,
        fs::{FileTypeExt, MetadataExt, OpenOptionsExt},
        net::{SocketAddr, UnixDatagram},
    },
    path::{Path, PathBuf},
};

const SOCKET_ROOT: &str = "AGENT_RUN_TEST_SOCKET_TMP";
const TEMP_NAMES: [&str; 3] = ["TMPDIR", "TMP", "TEMP"];

/// Path-only checks sent to the existing authenticated system worker. This
/// carries no task, environment, token or provider credentials.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessPlan {
    workdir: PathBuf,
    reads: Vec<PathBuf>,
    writes: Vec<PathBuf>,
    writable: Vec<PathBuf>,
    sockets: Vec<PathBuf>,
    tmpdir: Option<PathBuf>,
    write_workdir: bool,
    needs_git_commit: bool,
    shell: bool,
    network: bool,
    codex: bool,
    projects: bool,
}

/// Observed launch facts. Only advisory messages cross the admission boundary.
pub(crate) struct Facts {
    pub(crate) warnings: Vec<String>,
    tmpdir: Option<PathBuf>,
    socket_root: Option<PathBuf>,
    git_blocker: Option<String>,
}

impl AccessPlan {
    /// Compile the same role and native settings that will be used at launch.
    pub(crate) fn compile(
        config: &ProviderConfig,
        harness: HarnessId,
        request: &ProviderStartRequest,
        effective: &StartRequest,
        role: &Profile,
        app_home: &Path,
    ) -> Result<Self> {
        let codex = harness == HarnessId::Codex;
        let managed = config
            .managed_attempt_binding
            .as_ref()
            .is_some_and(|settings| settings.enabled && settings.role_name == role.name);
        let shell = !managed && !role.research_tools_only() && (codex || role.write);
        let mut projects = false;
        let (writable, network) = match harness {
            HarnessId::Codex => {
                let runtime =
                    agent_run_adapters::provider::runtime(config, harness, &request.model)?;
                let grant = crate::codex::Grant::new(&runtime, effective, role, app_home)?;
                projects = grant.permission_profile.as_deref() == Some("Projects");
                (
                    grant.scope_writable_roots(&runtime, app_home),
                    grant.network_access,
                )
            }
            HarnessId::ClaudeCode => {
                let write =
                    agent_run_adapters::provider::claude_edit_root(role.write, &request.workdir);
                (write.into_iter().map(Path::to_path_buf).collect(), shell)
            }
            HarnessId::OpenCode => (
                role.write
                    .then(|| request.workdir.clone())
                    .into_iter()
                    .collect(),
                shell,
            ),
        };
        Ok(Self {
            workdir: request.workdir.clone(),
            reads: effective
                .read_roots
                .iter()
                .chain(&request.read_roots)
                .cloned()
                .collect(),
            writes: request.write_roots.clone(),
            writable,
            sockets: request.sockets.clone(),
            tmpdir: request.tmpdir.clone(),
            write_workdir: role.write,
            needs_git_commit: request.needs_git_commit,
            shell,
            network,
            codex,
            projects,
        })
    }

    /// Check a plan as the local execution account. Probes never write files.
    pub fn warnings(&self) -> Result<Vec<String>> {
        Ok(self.inspect()?.warnings)
    }

    /// Recheck the scope at launch, then prepare workspace scratch as this UID.
    pub(crate) fn prepare(&self) -> Result<Facts> {
        let mut facts = self.inspect()?;
        if self.write_workdir {
            let scratch = self.workdir.join(".scratch");
            if facts.tmpdir.as_ref() == Some(&scratch) || self.scratch_available(&scratch) {
                if let Err(error) = self.create_scratch(&scratch) {
                    if facts.tmpdir.as_ref() == Some(&scratch) {
                        facts.tmpdir = None;
                    }
                    facts.warnings.push(error);
                } else {
                    self.ignore_scratch();
                }
            }
        }
        Ok(facts)
    }

    /// Validate inherited requirements and infer usable optional directories.
    fn inspect(&self) -> Result<Facts> {
        let workdir = directory(&self.workdir, "workdir", self.write_workdir)?;
        if workdir != self.workdir {
            return Err(invalid(format!(
                "workdir {:?} changed its canonical location; submit the physical directory path",
                self.workdir
            )));
        }
        if self.write_workdir {
            self.require_write(&workdir, "workdir")?;
        }
        for read in &self.reads {
            let canonical = directory(read, "read root", false)?;
            if self.projects && projects_denies(&canonical) {
                return Err(invalid(format!(
                    "read root {read:?} is blocked by the native Projects policy; use an approved read directory"
                )));
            }
        }
        for write in &self.writes {
            let canonical = directory(write, "write root", true)?;
            self.require_write(&canonical, "write root")?;
        }
        let git_blocker = self.git_blocker();
        if self.needs_git_commit
            && let Some(reason) = &git_blocker
        {
            return Err(invalid(reason.clone()));
        }
        for socket in &self.sockets {
            self.check_socket(socket)?;
        }

        let mut warnings = Vec::new();
        let scratch = self.workdir.join(".scratch");
        let tmpdir = if let Some(tmpdir) = &self.tmpdir {
            let canonical = directory(tmpdir, "temporary directory", true)?;
            self.require_write(&canonical, "temporary directory")?;
            Some(canonical)
        } else if self.scratch_available(&scratch) {
            Some(scratch.clone())
        } else {
            if self.write_workdir {
                warnings.push(format!(
                    "Scratch directory {scratch:?} is unavailable; use tmpdir with an existing writable directory or repair .scratch."
                ));
            }
            None
        };
        let socket_root = self.test_socket_root(tmpdir.as_deref(), &mut warnings);
        Ok(Facts {
            warnings,
            tmpdir,
            socket_root,
            git_blocker,
        })
    }

    /// Use only real directories covered by the compiled write grant.
    fn require_write(&self, path: &Path, kind: &str) -> Result<()> {
        if !self.admits_write(path) {
            return Err(invalid(format!(
                "{kind} {path:?} is outside the native write grant; use {:?} or ask the operator to configure a writable workspace root",
                self.workdir.join(".scratch")
            )));
        }
        Ok(())
    }

    /// Check a canonical location against the effective native write roots.
    fn admits_write(&self, path: &Path) -> bool {
        self.writable
            .iter()
            .any(|root| root.canonicalize().is_ok_and(|root| path.starts_with(root)))
            && !(self.projects && projects_denies(path))
            && !(self.codex
                && path.components().any(|part| {
                    matches!(
                        part.as_os_str().to_str(),
                        Some(".git" | ".codex" | ".agents")
                    )
                }))
    }

    /// A symlink or incompatible existing directory never becomes scratch.
    fn scratch_available(&self, scratch: &Path) -> bool {
        self.write_workdir
            && self.admits_write(scratch)
            && match fs::symlink_metadata(scratch) {
                Ok(meta) => {
                    meta.is_dir()
                        && meta.mode() & 0o007 == 0
                        && access(scratch, libc::R_OK | libc::W_OK | libc::X_OK)
                }
                Err(e) => e.kind() == std::io::ErrorKind::NotFound,
            }
    }

    /// Create a real group-writable directory under the worktree. An existing
    /// directory's mode and ownership are left alone.
    fn create_scratch(&self, scratch: &Path) -> std::result::Result<(), String> {
        let failure =
            || format!("Could not prepare {scratch:?}; repair .scratch or supply tmpdir.");
        let parent =
            agent_run_platform::fs::Dir::open_searchable(&self.workdir).map_err(|_| failure())?;
        // mkdir is relative to the pinned workspace and never follows the final entry.
        parent
            .scratch_directory(Path::new(".scratch"))
            .map_err(|_| failure())?;
        if !self.scratch_available(scratch) {
            return Err(failure());
        }
        Ok(())
    }

    /// Add a local exclude when the worktree owns its metadata. A linked
    /// worktree's external metadata is left to its owner.
    fn ignore_scratch(&self) {
        let result = (|| -> Result<()> {
            let common = git_dirs(&self.workdir)?.1;
            if !common.starts_with(&self.workdir) {
                return Ok(());
            }
            let info = agent_run_platform::fs::Dir::open_searchable(&common.join("info"))?;
            info.exclude_scratch()?;
            Ok(())
        })();
        let _ = result;
    }

    /// Explain why a handoff must commit as the orchestrator, if applicable.
    fn git_blocker(&self) -> Option<String> {
        let (gitdir, common) = match git_dirs(&self.workdir) {
            Ok(dirs) => dirs,
            Err(agent_run_domain::Error::Validation(reason)) => return Some(reason),
            Err(_) => {
                return Some(
                    "Git metadata could not be checked; commit as the orchestrator.".into(),
                );
            }
        };
        for path in [&gitdir, &common] {
            if !self.write_workdir || !self.admits_write(path) || self.codex {
                return Some(format!(
                    "Git metadata {path:?} is outside the native write grant or protected by the sandbox; commit as the orchestrator."
                ));
            }
            if !access(path, libc::R_OK | libc::W_OK | libc::X_OK) {
                return Some(format!(
                    "Git metadata {path:?} is not writable by the worker UID; grant that account access or commit as the orchestrator."
                ));
            }
        }
        None
    }

    /// Check a pathname without sending any data to the endpoint.
    fn check_socket(&self, path: &Path) -> Result<()> {
        if SocketAddr::from_pathname(path).is_err() {
            return Err(invalid(format!(
                "Unix socket {path:?} is too long; place it under a shorter directory."
            )));
        }
        if !fs::metadata(path).is_ok_and(|metadata| metadata.file_type().is_socket()) {
            return Err(invalid(format!(
                "Unix socket {path:?} is missing or unreachable by the worker UID; start its service and grant ancestor traversal."
            )));
        }
        if !self.shell || (self.codex && !self.network) {
            return Err(invalid(format!(
                "Unix socket {path:?} is blocked by the native shell/network grant; select a role and harness with local IPC access."
            )));
        }
        #[cfg(target_os = "linux")]
        if !access(path, libc::W_OK) {
            return Err(invalid(format!(
                "Unix socket {path:?} is not connectable by the worker UID; grant the worker socket write permission."
            )));
        }
        // A nonblocking connect also catches stale socket inodes and busy
        // listener queues. Datagram sockets have no accept handshake.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()?;
        let stream = runtime.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_millis(250),
                tokio::net::UnixStream::connect(path),
            )
            .await
        });
        if matches!(stream, Ok(Ok(_)))
            || matches!(stream, Ok(Err(ref error)) if error.raw_os_error() == Some(libc::EPROTOTYPE))
                && UnixDatagram::unbound()
                    .and_then(|socket| socket.connect(path))
                    .is_ok()
        {
            return Ok(());
        }
        Err(invalid(format!(
            "Unix socket {path:?} did not accept a worker connection; verify the service is listening and the worker can connect."
        )))
    }

    /// Select a short canonical socket base only when it is safe for the
    /// repository's operator-socket tests. The host can provision a short base.
    fn test_socket_root(
        &self,
        tmpdir: Option<&Path>,
        warnings: &mut Vec<String>,
    ) -> Option<PathBuf> {
        if !self.write_workdir {
            return None;
        }
        let override_path = std::env::var_os(SOCKET_ROOT).map(PathBuf::from);
        let candidates = override_path
            .iter()
            .map(PathBuf::as_path)
            .chain(tmpdir.filter(|path| path.is_dir()))
            .chain(std::iter::once(self.workdir.as_path()))
            .chain(self.writes.iter().map(PathBuf::as_path))
            .chain(self.writable.iter().map(PathBuf::as_path));
        for candidate in candidates {
            if candidate.as_os_str().as_bytes().len() <= 50
                && candidate.canonicalize().ok().as_deref() == Some(candidate)
                && self.admits_write(candidate)
                && access(candidate, libc::R_OK | libc::W_OK | libc::X_OK)
                && protected_socket_base(candidate)
            {
                return Some(candidate.to_path_buf());
            }
        }
        if override_path.is_some() {
            warnings.push(format!(
                "{SOCKET_ROOT} is not a short protected writable directory; provision a canonical socket test root with protected ancestors."
            ));
        } else if tmpdir.is_some_and(|path| {
            !protected_socket_base(path) || path.as_os_str().as_bytes().len() > 50
        }) {
            warnings.push(format!(
                "Socket tests may need {SOCKET_ROOT} set to a short writable directory with protected ancestors."
            ));
        }
        None
    }

    /// Render the sandbox facts ahead of the frozen worker instructions.
    pub(crate) fn brief(
        &self,
        facts: &Facts,
        explicit_finish: bool,
        configured_tmp: bool,
        harness: HarnessId,
    ) -> String {
        let tmp = if configured_tmp && self.tmpdir.is_none() {
            "harness environment override".into()
        } else {
            facts
                .tmpdir
                .as_ref()
                .map_or_else(|| "native default".into(), |path| format!("{path:?}"))
        };
        let git = facts
            .git_blocker
            .as_ref()
            .map_or_else(|| "yes".into(), |reason| format!("no — {reason}"));
        let network = if self.network { "yes" } else { "no" };
        let finish = if explicit_finish {
            let tool = match harness {
                HarnessId::OpenCode => "agent_run_worker_finish",
                _ => "mcp__agent_run_worker__finish",
            };
            format!("Call {tool} with status and summary, then end the turn without text.")
        } else {
            "Return the final response through the native harness.".into()
        };
        let mut brief = format!(
            "## Sandbox facts\n- Workdir: {:?}\n- Writable roots: {:?}\n- TMPDIR: {tmp}\n- Git commit: {git}\n- Shell network: {network}\n- Finish: {finish}\n",
            self.workdir, self.writable,
        );
        if let Some(path) = &facts.socket_root {
            brief.push_str(&format!("- {SOCKET_ROOT}: {path:?}\n"));
        }
        for warning in &facts.warnings {
            brief.push_str(&format!("- {warning}\n"));
        }
        brief
    }
}

impl Facts {
    /// Expose this one nonsecret path to Codex shells even when their sealed
    /// policy uses core inheritance or excludes private AGENT_RUN variables.
    pub(crate) fn codex_socket_override(&self) -> Option<String> {
        self.socket_root.as_ref().map(|path| {
            let value = toml::Value::String(path.to_string_lossy().into_owned());
            format!("shell_environment_policy.set.{SOCKET_ROOT}={value}")
        })
    }

    /// Apply the task override or default, preserving explicit harness settings.
    pub(crate) fn install_environment(
        &self,
        plan: &AccessPlan,
        environment: &mut BTreeMap<String, String>,
        configured_tmp: bool,
    ) {
        if let Some(path) = &self.tmpdir
            && (!configured_tmp || plan.tmpdir.is_some())
        {
            for name in TEMP_NAMES {
                environment.insert(name.into(), path.to_string_lossy().into_owned());
            }
        }
        if let Some(path) = &self.socket_root {
            environment.insert(SOCKET_ROOT.into(), path.to_string_lossy().into_owned());
        } else if plan.write_workdir {
            environment.remove(SOCKET_ROOT);
        }
    }
}

/// Check whether a developer environment explicitly overrides the temp default.
pub(crate) fn configured_tmp(config: &ProviderConfig, harness: HarnessId) -> bool {
    config
        .harnesses
        .get(&harness)
        .and_then(|native| native.environment.as_ref())
        .and_then(|name| config.environments.get(name))
        .is_some_and(|env| {
            TEMP_NAMES
                .iter()
                .any(|name| env.variables.contains_key(*name))
        })
}

/// Matches the sensitive path patterns of the generated Projects policy.
fn projects_denies(path: &Path) -> bool {
    path.components().any(|part| {
        part.as_os_str().to_str().is_some_and(|name| {
            name == ".env"
                || name.starts_with(".env.")
                || name.ends_with(".pem")
                || name.ends_with(".key")
        })
    })
}

/// Resolve and check a directory with the actual process credentials and ACLs.
fn directory(path: &Path, kind: &str, write: bool) -> Result<PathBuf> {
    let mode = libc::X_OK | libc::R_OK | if write { libc::W_OK } else { 0 };
    if path.is_absolute()
        && access(path, mode)
        && let Ok(canonical) = path.canonicalize()
        && canonical.is_dir()
    {
        return Ok(canonical);
    }
    let permission = if write {
        "readable and writable"
    } else {
        "readable"
    };
    Err(invalid(format!(
        "{kind} {path:?} must be an existing {permission} directory for the worker UID; grant directory traversal and access or choose a different path."
    )))
}

/// Use the kernel access decision for the effective credentials and groups.
fn access(path: &Path, mode: libc::c_int) -> bool {
    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: the C string lives for the call and the flags only query access.
    unsafe { libc::faccessat(libc::AT_FDCWD, path.as_ptr(), mode, libc::AT_EACCESS) == 0 }
}

/// Decode Git's bounded text pointer files without invoking Git configuration.
fn git_dirs(workdir: &Path) -> Result<(PathBuf, PathBuf)> {
    let dotgit = workdir
        .ancestors()
        .map(|path| path.join(".git"))
        .find(|path| fs::symlink_metadata(path).is_ok())
        .unwrap_or_else(|| workdir.join(".git"));
    let unavailable = || {
        invalid(format!(
            "Git metadata {dotgit:?} cannot be resolved; use a Git worktree with readable metadata or commit as the orchestrator."
        ))
    };
    let meta = fs::symlink_metadata(&dotgit).map_err(|_| unavailable())?;
    let gitdir = if meta.is_file() {
        let text = git_pointer(&dotgit).ok_or_else(unavailable)?;
        let target = text.strip_prefix("gitdir: ").ok_or_else(unavailable)?;
        dotgit.parent().ok_or_else(unavailable)?.join(target)
    } else if meta.is_dir() {
        dotgit.clone()
    } else {
        return Err(unavailable());
    };
    let gitdir = directory(&gitdir, "Git metadata", false).map_err(|_| unavailable())?;
    let common = match fs::symlink_metadata(gitdir.join("commondir")) {
        Ok(_) => {
            let relative = git_pointer(&gitdir.join("commondir")).ok_or_else(unavailable)?;
            directory(&gitdir.join(relative), "shared Git metadata", false)
                .map_err(|_| unavailable())?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => gitdir.clone(),
        Err(_) => return Err(unavailable()),
    };
    Ok((gitdir, common))
}

/// Read one short regular Git pointer, with no symlink or blocking FIFO open.
fn git_pointer(path: &Path) -> Option<String> {
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .ok()?;
    if !file.metadata().ok()?.is_file() {
        return None;
    }
    let mut text = String::new();
    file.take(4097).read_to_string(&mut text).ok()?;
    let text = text.trim_end_matches(['\r', '\n']);
    (!text.is_empty() && text.len() <= 4096 && !text.chars().any(char::is_control))
        .then(|| text.to_owned())
}

/// A Linux operator fixture must be able to traverse clean, protected ancestors.
#[cfg(target_os = "linux")]
fn protected_socket_base(path: &Path) -> bool {
    // SAFETY: these calls only query the account's effective credentials.
    let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
    path.ancestors().all(|ancestor| {
        fs::symlink_metadata(ancestor).is_ok_and(|meta| {
            meta.is_dir()
                && [0, uid].contains(&meta.uid())
                && meta.mode() & 0o022 == 0
                && meta.mode() & 0o010 != 0
                && (meta.gid() == gid || meta.mode() & 0o001 != 0)
        }) && without_acl(ancestor, c"system.posix_acl_access")
    }) && without_acl(path, c"system.posix_acl_default")
}

/// Check for ACL presence without reading or exposing the entries.
#[cfg(target_os = "linux")]
fn without_acl(path: &Path, name: &std::ffi::CStr) -> bool {
    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: both C strings are live and the null buffer requests size only.
    (unsafe { libc::lgetxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) < 0 })
        && matches!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ENODATA | libc::EOPNOTSUPP)
        )
}

/// macOS requires a short canonical path but has no Linux operator ACL rule.
#[cfg(not(target_os = "linux"))]
fn protected_socket_base(_: &Path) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::{
        fs::{PermissionsExt, symlink},
        net::UnixListener,
    };

    /// Construct the path plan of a writable native role in an isolated workspace.
    fn plan(workdir: &Path) -> AccessPlan {
        let workdir = workdir.canonicalize().unwrap();
        AccessPlan {
            reads: vec![],
            writes: vec![],
            writable: vec![workdir.clone()],
            sockets: vec![],
            tmpdir: None,
            workdir,
            write_workdir: true,
            needs_git_commit: false,
            shell: true,
            network: true,
            codex: false,
            projects: false,
        }
    }

    /// Effective worker access checks include directory search and write access.
    #[test]
    fn inaccessible_requested_paths_are_refused() {
        // SAFETY: read-only process identity lookup.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let mut check = plan(root.path());
        let readonly = root.path().join("readonly");
        fs::create_dir(&readonly).unwrap();
        fs::set_permissions(&readonly, fs::Permissions::from_mode(0o500)).unwrap();
        check.writes.push(readonly.clone());
        let error = check.warnings().err().unwrap().to_string();
        assert!(error.contains(readonly.to_str().unwrap()) && error.contains("worker UID"));
        check.writes.clear();
        check.reads.push(readonly.clone());
        fs::set_permissions(&readonly, fs::Permissions::from_mode(0o400)).unwrap();
        let error = check.warnings().err().unwrap().to_string();
        assert!(error.contains("read root") && error.contains("traversal"));
    }

    /// Scope requirements and symlink aliases cannot widen a native grant.
    #[test]
    fn write_root_and_tmpdir_must_stay_inside_native_grant() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let mut check = plan(root.path());
        let alias = check.workdir.join("alias");
        symlink(outside.path(), &alias).unwrap();
        check.writes.push(alias);
        let error = check.warnings().err().unwrap().to_string();
        assert!(error.contains(outside.path().canonicalize().unwrap().to_str().unwrap()));
        assert!(error.contains("outside the native write grant"));
        check.writes.clear();
        check.tmpdir = Some(outside.path().into());
        let error = check.warnings().err().unwrap().to_string();
        assert!(error.contains("temporary directory") && error.contains(".scratch"));
        check.tmpdir = None;
        let denied = check.workdir.join(".env");
        fs::create_dir(&denied).unwrap();
        check.projects = true;
        check.reads.push(denied);
        assert!(
            check
                .warnings()
                .err()
                .unwrap()
                .to_string()
                .contains("Projects policy")
        );
    }

    /// Resolve both Git directories, including a distinct relative commondir.
    #[test]
    fn linked_git_metadata_outside_grant_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let mut check = plan(root.path());
        let gitdir = check.workdir.join("linked");
        let common = tempfile::tempdir().unwrap();
        fs::create_dir(&gitdir).unwrap();
        fs::write(check.workdir.join(".git"), "gitdir: linked\n").unwrap();
        fs::write(
            gitdir.join("commondir"),
            format!("{}\n", common.path().display()),
        )
        .unwrap();
        check.needs_git_commit = true;
        let error = check.warnings().err().unwrap().to_string();
        assert!(error.contains(common.path().canonicalize().unwrap().to_str().unwrap()));
        assert!(error.contains("commit as the orchestrator"));
        check.writable.push(common.path().into());
        assert!(check.warnings().is_ok());
        check.codex = true;
        let error = check.warnings().err().unwrap().to_string();
        assert!(error.contains("protected by the sandbox"));
    }

    /// Detect missing, stale and network-denied endpoints before admission.
    #[test]
    fn socket_scope_requires_a_reachable_permitted_endpoint() {
        let root = tempfile::tempdir().unwrap();
        let mut check = plan(root.path());
        let path = root.path().join("ipc.sock");
        check.sockets.push(path.clone());
        assert!(
            check
                .warnings()
                .err()
                .unwrap()
                .to_string()
                .contains("missing or unreachable")
        );
        let listener = UnixListener::bind(&path).unwrap();
        assert!(check.warnings().is_ok());
        check.codex = true;
        check.network = false;
        assert!(
            check
                .warnings()
                .err()
                .unwrap()
                .to_string()
                .contains("shell/network grant")
        );
        check.network = true;
        drop(listener);
        assert!(
            check
                .warnings()
                .err()
                .unwrap()
                .to_string()
                .contains("did not accept")
        );
    }

    /// Admission leaves the workspace untouched, then the worker prepares
    /// scratch, ignores it locally and receives actual paths in its brief.
    #[test]
    fn scratch_environment_and_brief_use_verified_workspace() {
        let root = tempfile::tempdir().unwrap();
        let mut check = plan(root.path());
        let git_info = check.workdir.join(".git/info");
        fs::create_dir_all(&git_info).unwrap();
        let exclude = git_info.join("exclude");
        fs::write(&exclude, "existing\n").unwrap();
        let scratch = check.workdir.join(".scratch");
        check.needs_git_commit = true;
        check.warnings().unwrap();
        assert!(!scratch.exists());
        let facts = check.prepare().unwrap();
        assert!(scratch.is_dir());
        assert_eq!(fs::metadata(&scratch).unwrap().mode() & 0o007, 0);
        check.prepare().unwrap();
        assert_eq!(
            fs::read_to_string(exclude)
                .unwrap()
                .matches("/.scratch/")
                .count(),
            1
        );
        let mut environment = BTreeMap::new();
        facts.install_environment(&check, &mut environment, false);
        for name in TEMP_NAMES {
            assert_eq!(environment[name], scratch.to_str().unwrap());
        }
        let brief = check.brief(&facts, true, false, HarnessId::ClaudeCode);
        assert!(brief.starts_with("## Sandbox facts"));
        assert!(brief.contains("Git commit: yes"));
        assert!(brief.contains("mcp__agent_run_worker__finish"));
        assert!(brief.contains(&format!("TMPDIR: {scratch:?}")));

        let custom = check.workdir.join("custom");
        fs::create_dir(&custom).unwrap();
        check.tmpdir = Some(custom.clone());
        check
            .prepare()
            .unwrap()
            .install_environment(&check, &mut environment, true);
        assert_eq!(environment["TMPDIR"], custom.to_str().unwrap());
    }

    /// An unsafe scratch entry yields a warning and its link target stays untouched.
    #[test]
    fn scratch_conflict_is_advisory() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let check = plan(root.path());
        symlink(outside.path(), check.workdir.join(".scratch")).unwrap();
        let facts = check.prepare().unwrap();
        assert!(facts.tmpdir.is_none());
        assert!(
            facts
                .warnings
                .iter()
                .any(|warning| warning.contains("repair .scratch"))
        );
        assert!(fs::read_dir(outside.path()).unwrap().next().is_none());
    }
}
