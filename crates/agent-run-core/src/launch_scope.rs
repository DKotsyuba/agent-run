//! Worker-identity access checks for provider admission and actual launch.

use crate::{Result, profiles::Profile};
use agent_run_config::provider_config::ProviderConfig;
use agent_run_domain::{
    ProviderStartRequest,
    catalog::HarnessId,
    domain::{AgentId, StartRequest},
    error::invalid,
};
use agent_run_platform::fs::{Dir, EntryType};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CString,
    fs,
    os::unix::{
        ffi::OsStrExt,
        fs::{FileTypeExt, MetadataExt},
        net::{SocketAddr, UnixDatagram},
    },
    path::{Component, Path, PathBuf},
};

const SOCKET_ROOT: &str = "AGENT_RUN_TEST_SOCKET_TMP";
const SCOPE_REFUSAL: &str = "Requested path is outside the authorized worker scope.";
const TEMP_REFUSAL: &str = "Could not verify a protected worker temporary directory; check worker ownership and ancestor permissions.";
const DIRECTORY_ACCESS: libc::c_int = libc::R_OK | libc::X_OK;
const DIRECTORY_WRITE_ACCESS: libc::c_int = DIRECTORY_ACCESS | libc::W_OK;
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

    /// Keep only native write roots relevant to this system probe. Codex
    /// protects Git metadata and the other harnesses only grant the workdir.
    /// Unrelated operator-configured roots never consume the wire budget.
    pub fn for_system_probe(mut self) -> Self {
        if self.codex {
            let mut relevant = BTreeSet::new();
            for path in std::iter::once(&self.workdir)
                .chain(&self.writes)
                .chain(&self.sockets)
                .chain(&self.tmpdir)
            {
                if let Some(root) = self.writable.iter().find(|root| in_root(path, root)) {
                    relevant.insert(root.clone());
                }
            }
            self.writable = relevant.into_iter().collect();
        }
        self
    }

    /// Check a plan as the local execution account. Probes never write files.
    pub fn warnings(&self) -> Result<Vec<String>> {
        Ok(self.inspect(false)?.warnings)
    }

    /// Check a broker-authenticated plan without returning private path facts.
    pub fn system_warnings(&self) -> Result<Vec<String>> {
        Ok(self.inspect(true)?.warnings)
    }

    /// Recheck access and create temp storage. A routed worker can receive
    /// requests from another UID, so never export workspace scratch there.
    pub(crate) fn prepare(&self, system_run: Option<(&Path, &AgentId)>) -> Result<Facts> {
        let mut facts = self.inspect(system_run.is_some())?;
        if let Some((private_home, id)) = system_run {
            let base = self.tmpdir.as_deref().unwrap_or(private_home);
            let parent = open_run_temp_base(base)?;
            facts.tmpdir = Some(create_run_temp(base, &parent, id)?);
            // Only launch has the complete grant and its verified temp path.
            facts.socket_root =
                self.test_socket_root(facts.tmpdir.as_deref(), &mut facts.warnings, true);
        } else if self.write_workdir {
            // Direct launches share the caller's UID and its rename authority.
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
    fn inspect(&self, system: bool) -> Result<Facts> {
        if system
            && !(self
                .writes
                .iter()
                .chain(&self.tmpdir)
                .all(|path| self.admits_system_write(path))
                && self
                    .reads
                    .iter()
                    .chain(&self.sockets)
                    .all(|path| self.admits_system_read(path)))
        {
            return Err(invalid(SCOPE_REFUSAL));
        }
        let workdir =
            self.inspect_directory(&self.workdir, "workdir", self.write_workdir, system)?;
        if workdir != self.workdir {
            return Err(invalid(
                "workdir changed its canonical location; submit the physical directory path",
            ));
        }
        for read in &self.reads {
            self.inspect_directory(read, "read root", false, system)?;
        }
        for write in &self.writes {
            self.inspect_directory(write, "write root", true, system)?;
        }
        let git_blocker = self.git_blocker(system);
        if self.needs_git_commit
            && let Some(reason) = &git_blocker
        {
            return Err(invalid(reason.clone()));
        }
        for socket in &self.sockets {
            self.check_socket(socket, system)?;
        }

        let mut warnings = Vec::new();
        let scratch = self.workdir.join(".scratch");
        let tmpdir = if let Some(tmpdir) = &self.tmpdir {
            let path = self.inspect_directory(tmpdir, "temporary directory", true, system)?;
            if system {
                open_run_temp_base(&path)?;
            }
            Some(path)
        } else if system {
            None
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
        let socket_root = if system {
            None
        } else {
            self.test_socket_root(tmpdir.as_deref(), &mut warnings, false)
        };
        Ok(Facts {
            warnings,
            tmpdir,
            socket_root,
            git_blocker,
        })
    }

    /// Check system-worker scope lexically before opening any path, then
    /// traverse without symlinks and test access through the opened descriptor.
    fn inspect_directory(
        &self,
        path: &Path,
        kind: &str,
        write: bool,
        system: bool,
    ) -> Result<PathBuf> {
        if system {
            let admitted = if write {
                self.admits_system_write(path)
            } else {
                self.admits_system_read(path)
            };
            if !admitted {
                return Err(invalid(SCOPE_REFUSAL));
            }
            if Dir::open_searchable(path).is_ok_and(|dir| {
                dir.has_access(if write {
                    DIRECTORY_WRITE_ACCESS
                } else {
                    DIRECTORY_ACCESS
                })
            }) {
                return Ok(path.to_path_buf());
            }
            if kind == "workdir" {
                return Err(invalid(
                    "Workdir is unavailable to the worker UID; verify directory access and ancestor traversal.",
                ));
            }
            return Err(invalid(SCOPE_REFUSAL));
        }
        let canonical = directory(path, kind, write)?;
        if write && !self.admits_write(&canonical) {
            return Err(invalid(format!(
                "{kind} {path:?} is outside the native write grant; use {:?} or ask the operator to configure a writable workspace root",
                self.workdir.join(".scratch")
            )));
        }
        if !write && self.projects && projects_denies(&canonical) {
            return Err(invalid(format!(
                "{kind} {path:?} is blocked by the native Projects policy; use an approved read directory"
            )));
        }
        Ok(canonical)
    }

    /// Match physical locations against the native write policy for direct launches.
    fn admits_write(&self, path: &Path) -> bool {
        self.writable
            .iter()
            .any(|root| root.canonicalize().is_ok_and(|root| path.starts_with(root)))
            && self.native_write_path(path)
    }

    /// Check native protected paths before doing any worker-side filesystem I/O.
    fn admits_system_write(&self, path: &Path) -> bool {
        self.writable.iter().any(|root| in_root(path, root)) && self.native_write_path(path)
    }

    /// Only the workspace and the selected role's read and write grants
    /// can authorize a system-worker directory or endpoint probe.
    fn admits_system_read(&self, path: &Path) -> bool {
        (in_root(path, &self.workdir)
            || self
                .reads
                .iter()
                .chain(&self.writable)
                .any(|root| in_root(path, root)))
            && !(self.projects && projects_denies(path))
    }

    /// Matches sensitive paths protected by Codex even within a writable root.
    fn native_write_path(&self, path: &Path) -> bool {
        !(self.projects && projects_denies(path)
            || self.codex
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
            let common = self
                .git_dirs(false)
                .ok_or_else(|| invalid("Git metadata unavailable"))?
                .1;
            if !common.starts_with(&self.workdir) {
                return Ok(());
            }
            let info = agent_run_platform::fs::Dir::open_searchable(&common.join("info"))?;
            info.exclude_scratch()?;
            Ok(())
        })();
        let _ = result;
    }

    /// Check the necessary Git write paths using the worker's effective access.
    fn git_blocker(&self, system: bool) -> Option<String> {
        let refuse = |why| Some(format!("{why}; commit as the orchestrator."));
        if !self.write_workdir || self.codex {
            return refuse(
                "Git metadata is outside the native write grant or protected by the sandbox",
            );
        }
        let Some((gitdir, common)) = self.git_dirs(system) else {
            return refuse("Git metadata could not be resolved inside the native write grant");
        };
        let writable = |path: &Path| {
            self.inspect_directory(path, "Git metadata", true, system)
                .is_ok()
        };
        if ![&gitdir, &common, &common.join("objects")]
            .into_iter()
            .all(|path| writable(path))
        {
            return refuse("Git metadata or objects directory is not writable by the worker UID");
        }
        let Ok(git) = Dir::open_searchable(&gitdir) else {
            return refuse("Git metadata cannot be opened by the worker UID");
        };
        let Some(head) = git_pointer(&git, Path::new("HEAD")) else {
            return refuse("Git HEAD cannot be read by the worker UID");
        };
        match git.entry(Some(Path::new("index.lock"))) {
            Err(agent_run_domain::Error::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return refuse("Git index.lock is present or cannot be checked"),
        }
        if let Some(reference) = head.strip_prefix("ref: ") {
            let reference = Path::new(reference);
            if !in_root(reference, Path::new("refs")) || reference == Path::new("refs") {
                return refuse("Git HEAD does not name a supported reference");
            }
            let Ok(metadata) = Dir::open_searchable(&common) else {
                return refuse("Shared Git metadata cannot be opened by the worker UID");
            };
            let mut parent = reference.parent().unwrap_or(Path::new("refs"));
            loop {
                match metadata.entry(Some(parent)) {
                    Ok(entry)
                        if entry.kind == EntryType::Directory && writable(&common.join(parent)) =>
                    {
                        break;
                    }
                    Err(agent_run_domain::Error::Io(error))
                        if error.kind() == std::io::ErrorKind::NotFound
                            && parent != Path::new("refs") =>
                    {
                        parent = parent.parent().unwrap_or(Path::new("refs"));
                    }
                    _ => {
                        return refuse(
                            "Git HEAD reference directory is not writable by the worker UID",
                        );
                    }
                }
            }
            for file in [reference, Path::new("packed-refs")] {
                match metadata.entry(Some(file)) {
                    Ok(entry)
                        if entry.kind == EntryType::File && metadata.open_file(file).is_ok() => {}
                    Err(agent_run_domain::Error::Io(error))
                        if error.kind() == std::io::ErrorKind::NotFound => {}
                    _ => return refuse("Git reference metadata cannot be read by the worker UID"),
                }
            }
        } else if !matches!(head.len(), 40 | 64)
            || !head.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return refuse("Git HEAD is not a supported symbolic or detached reference");
        }
        None
    }

    /// Resolve Git's linked-worktree pointers inside authorized, no-follow
    /// directories without invoking Git configuration or hooks.
    fn git_dirs(&self, system: bool) -> Option<(PathBuf, PathBuf)> {
        let (path, dotgit, parent) = self.find_dotgit(system)?;
        let check = |path: &Path| {
            if system && !self.admits_system_write(path) {
                return None;
            }
            Dir::open_searchable(path).ok()
        };
        let gitdir = match dotgit {
            EntryType::Directory => path.join(".git"),
            EntryType::File => {
                let pointer = git_pointer(&parent, Path::new(".git"))?;
                git_target(path, pointer.strip_prefix("gitdir: ")?)?
            }
            _ => return None,
        };
        let dir = check(&gitdir)?;
        let name = Path::new("commondir");
        let common = match dir.entry(Some(name)) {
            Ok(_) => {
                let path = git_target(&gitdir, &git_pointer(&dir, name)?)?;
                check(&path)?;
                path
            }
            Err(agent_run_domain::Error::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                gitdir.clone()
            }
            Err(_) => return None,
        };
        Some((gitdir, common))
    }

    /// Search only granted ancestors, stopping on an unreadable metadata entry.
    fn find_dotgit(&self, system: bool) -> Option<(&Path, EntryType, Dir)> {
        for path in self.workdir.ancestors() {
            if system && !self.admits_system_write(path) {
                break;
            }
            let parent = Dir::open_searchable(path).ok()?;
            match parent.entry(Some(Path::new(".git"))) {
                Ok(entry) => return Some((path, entry.kind, parent)),
                Err(agent_run_domain::Error::Io(error))
                    if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => break,
            }
        }
        None
    }

    /// Verify only granted system endpoints. Linux pins the socket inode before
    /// connecting. Darwin requires stable parents because connect takes a path.
    fn check_socket(&self, path: &Path, system: bool) -> Result<()> {
        if system && !self.admits_system_read(path) {
            return Err(invalid(SCOPE_REFUSAL));
        }
        if !self.shell || (self.codex && !self.network) {
            let reason = "Unix socket is blocked by the native shell/network grant; select a role and harness with local IPC access.";
            return Err(invalid(if system {
                reason.into()
            } else {
                format!("{path:?}: {reason}")
            }));
        }
        let reason = |message| {
            if system {
                invalid(SCOPE_REFUSAL)
            } else {
                invalid(format!("Unix socket {path:?} {message}"))
            }
        };
        if SocketAddr::from_pathname(path).is_err() {
            return Err(reason("is too long; place it under a shorter directory."));
        }
        if !system && !fs::metadata(path).is_ok_and(|metadata| metadata.file_type().is_socket()) {
            return Err(reason(
                "is missing or unreachable by the worker UID; start its service and grant ancestor traversal.",
            ));
        }
        #[cfg(target_os = "linux")]
        let (_pinned, endpoint) = if system {
            use std::os::fd::AsRawFd as _;
            let socket = path
                .parent()
                .and_then(|parent| Dir::open_searchable(parent).ok())
                .and_then(|parent| parent.pin_socket(Path::new(path.file_name()?)).ok())
                .ok_or_else(|| invalid(SCOPE_REFUSAL))?;
            let endpoint = PathBuf::from(format!("/proc/self/fd/{}", socket.as_raw_fd()));
            (Some(socket), endpoint)
        } else {
            (None, path.to_path_buf())
        };
        #[cfg(target_os = "macos")]
        let endpoint = {
            if system {
                let parent = path
                    .parent()
                    .and_then(|parent| Dir::open_protected(parent).ok())
                    .ok_or_else(|| invalid(SCOPE_REFUSAL))?;
                if !parent
                    .entry(None)
                    .is_ok_and(|entry| entry.mode & 0o022 == 0)
                    || !parent
                        .entry(path.file_name().map(Path::new))
                        .is_ok_and(|entry| entry.socket)
                {
                    return Err(invalid(SCOPE_REFUSAL));
                }
            }
            path.to_path_buf()
        };
        #[cfg(target_os = "linux")]
        if !access(&endpoint, libc::W_OK) {
            return Err(reason(
                "is not connectable by the worker UID; grant the worker socket write permission.",
            ));
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
                tokio::net::UnixStream::connect(&endpoint),
            )
            .await
        });
        if matches!(stream, Ok(Ok(_)))
            || matches!(stream, Ok(Err(ref error)) if error.raw_os_error() == Some(libc::EPROTOTYPE))
                && UnixDatagram::unbound()
                    .and_then(|socket| socket.connect(&endpoint))
                    .is_ok()
        {
            return Ok(());
        }
        Err(reason(
            "did not accept a worker connection; verify the service is listening and the worker can connect.",
        ))
    }

    /// Select a short canonical socket base only when it is safe for the
    /// repository's operator-socket tests. The host can provision a short base.
    fn test_socket_root(
        &self,
        tmpdir: Option<&Path>,
        warnings: &mut Vec<String>,
        system: bool,
    ) -> Option<PathBuf> {
        if !self.write_workdir {
            return None;
        }
        let override_path = std::env::var_os(SOCKET_ROOT).map(PathBuf::from);
        let candidates = override_path
            .iter()
            .map(PathBuf::as_path)
            .chain(tmpdir)
            .chain(std::iter::once(self.workdir.as_path()))
            .chain(self.writes.iter().map(PathBuf::as_path))
            .chain(self.writable.iter().map(PathBuf::as_path));
        for candidate in candidates {
            if candidate.as_os_str().as_bytes().len() <= 50
                && (if system {
                    self.admits_system_write(candidate)
                        && Dir::open_protected(candidate)
                            .is_ok_and(|dir| dir.has_access(DIRECTORY_WRITE_ACCESS))
                } else {
                    candidate.canonicalize().ok().as_deref() == Some(candidate)
                        && self.admits_write(candidate)
                        && access(candidate, DIRECTORY_WRITE_ACCESS)
                })
                && protected_socket_base(candidate)
            {
                return Some(candidate.to_path_buf());
            }
        }
        if override_path.is_some() {
            warnings.push(format!(
                "{SOCKET_ROOT} is not a short protected writable directory; provision a canonical socket test root with protected ancestors."
            ));
        } else if !system
            && tmpdir.is_some_and(|path| {
                !protected_socket_base(path) || path.as_os_str().as_bytes().len() > 50
            })
        {
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
        temp_environment: &[(&str, String)],
        harness: HarnessId,
    ) -> String {
        let git = facts
            .git_blocker
            .as_ref()
            .map_or_else(
                || "required paths passed (Git config, hooks, object shards and later permission changes can still prevent a commit)".into(),
                |reason| format!("no — {reason}"),
            );
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
            "## Sandbox facts\n- Workdir: {:?}\n- Writable roots: {:?}\n- Git commit: {git}\n- Shell network: {network}\n- Finish: {finish}\n",
            self.workdir, self.writable,
        );
        for (name, source) in temp_environment {
            brief.push_str(&format!("- {name}: {source}\n"));
        }
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

    /// Merge temp settings independently and describe the effective entries
    /// without copying arbitrary inherited or harness values into the prompt.
    pub(crate) fn install_environment(
        &self,
        plan: &AccessPlan,
        environment: &mut BTreeMap<String, String>,
        configured_tmp: &[&str],
    ) -> [(&'static str, String); 3] {
        let temp_environment = TEMP_NAMES.map(|name| {
            let source = if let Some(path) = &self.tmpdir
                && (plan.tmpdir.is_some() || !configured_tmp.contains(&name))
            {
                environment.insert(name.into(), path.to_string_lossy().into_owned());
                format!("{path:?}")
            } else if environment.contains_key(name) {
                if configured_tmp.contains(&name) {
                    "harness environment override".into()
                } else {
                    "inherited environment".into()
                }
            } else {
                "unset (native default)".into()
            };
            (name, source)
        });
        if let Some(path) = &self.socket_root {
            environment.insert(SOCKET_ROOT.into(), path.to_string_lossy().into_owned());
        } else if plan.write_workdir {
            environment.remove(SOCKET_ROOT);
        }
        temp_environment
    }
}

/// Create a private temp leaf through a parent with verified rename authority.
fn create_run_temp(base: &Path, parent: &Dir, id: &AgentId) -> Result<PathBuf> {
    let name = format!(".agent-run-tmp-{id}");
    let expected = base.join(&name);
    if expected.to_str().is_none() {
        return Err(invalid(TEMP_REFUSAL));
    }
    let leaf = parent
        .private_child(Path::new(&name))
        .map_err(|_| invalid(TEMP_REFUSAL))?;
    if !leaf.has_access(DIRECTORY_WRITE_ACCESS) {
        return Err(invalid(TEMP_REFUSAL));
    }
    Ok(expected)
}

/// Admission and launch use the same protection and creation-permission checks.
fn open_run_temp_base(path: &Path) -> Result<Dir> {
    let dir = Dir::open_protected(path).map_err(|_| invalid(TEMP_REFUSAL))?;
    if path.to_str().is_none() || !dir.has_access(libc::W_OK | libc::X_OK) {
        return Err(invalid(TEMP_REFUSAL));
    }
    Ok(dir)
}

/// List the individual temp defaults explicitly overridden by the harness.
pub(crate) fn configured_tmp(config: &ProviderConfig, harness: HarnessId) -> Vec<&'static str> {
    config
        .harnesses
        .get(&harness)
        .and_then(|native| native.environment.as_ref())
        .and_then(|name| config.environments.get(name))
        .map_or_else(Vec::new, |env| {
            TEMP_NAMES
                .into_iter()
                .filter(|name| env.variables.contains_key(*name))
                .collect()
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

/// Reject traversal before matching paths against an authorized root.
fn in_root(path: &Path, root: &Path) -> bool {
    [path, root].into_iter().all(|path| {
        path.components()
            .all(|part| matches!(part, Component::RootDir | Component::Normal(_)))
    }) && path.starts_with(root)
}

/// Git uses leading parent components in linked worktree pointers. Only those
/// can be resolved without changing the meaning of a later symlink component.
fn git_target(base: &Path, pointer: &str) -> Option<PathBuf> {
    let path = Path::new(pointer);
    if path.is_absolute() {
        return in_root(path, Path::new("/")).then(|| path.to_path_buf());
    }
    let mut base = base.to_path_buf();
    let mut parts = path.components().peekable();
    while matches!(parts.peek(), Some(Component::CurDir | Component::ParentDir)) {
        if parts.next() == Some(Component::ParentDir) && !base.pop() {
            return None;
        }
    }
    for part in parts {
        if let Component::Normal(part) = part {
            base.push(part);
        } else {
            return None;
        }
    }
    Some(base)
}

/// Read a bounded Git pointer from a regular file using the pinned directory.
fn git_pointer(directory: &Dir, path: &Path) -> Option<String> {
    let bytes = directory.read(path, 4096).ok()?;
    let text = std::str::from_utf8(&bytes)
        .ok()?
        .trim_end_matches(['\r', '\n']);
    (!text.is_empty() && !text.chars().any(char::is_control)).then(|| text.to_owned())
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

    /// Minimal files-path Git backend with an unborn symbolic HEAD.
    fn git_fixture(gitdir: &Path, common: &Path) {
        fs::create_dir_all(common.join("objects")).unwrap();
        fs::create_dir_all(common.join("refs/heads")).unwrap();
        fs::write(gitdir.join("HEAD"), "ref: refs/heads/main\n").unwrap();
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

        let workdir = plan(&readonly);
        let error = workdir.system_warnings().unwrap_err().to_string();
        assert!(error.contains("Workdir is unavailable to the worker UID"));
        assert!(!error.contains(readonly.to_str().unwrap()));
    }

    /// Scope requirements and symlink aliases cannot widen a native grant.
    #[test]
    fn write_root_and_tmpdir_must_stay_inside_native_grant() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let mut check = plan(root.path());
        let alias = check.workdir.join("alias");
        symlink(outside.path(), &alias).unwrap();
        check.writes.push(alias.clone());
        let error = check.warnings().err().unwrap().to_string();
        assert!(error.contains(alias.to_str().unwrap()));
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
        git_fixture(&gitdir, common.path());
        check.needs_git_commit = true;
        let error = check.warnings().err().unwrap().to_string();
        assert!(!error.contains(common.path().canonicalize().unwrap().to_str().unwrap()));
        assert!(error.contains("commit as the orchestrator"));
        check.writable.push(common.path().canonicalize().unwrap());
        assert!(check.warnings().is_ok());
        assert!(check.system_warnings().is_ok());
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
        let git = check.workdir.join(".git");
        git_fixture(&git, &git);
        let exclude = git_info.join("exclude");
        fs::write(&exclude, "existing\n").unwrap();
        let scratch = check.workdir.join(".scratch");
        check.needs_git_commit = true;
        check.warnings().unwrap();
        assert!(!scratch.exists());
        let facts = check.prepare(None).unwrap();
        assert!(scratch.is_dir());
        assert_eq!(fs::metadata(&scratch).unwrap().mode() & 0o007, 0);
        check.prepare(None).unwrap();
        assert_eq!(
            fs::read_to_string(exclude)
                .unwrap()
                .matches("/.scratch/")
                .count(),
            1
        );
        let mut environment = BTreeMap::new();
        let temp_environment = facts.install_environment(&check, &mut environment, &[]);
        for name in TEMP_NAMES {
            assert_eq!(environment[name], scratch.to_str().unwrap());
        }
        let brief = check.brief(&facts, true, &temp_environment, HarnessId::ClaudeCode);
        assert!(brief.starts_with("## Sandbox facts"));
        assert!(brief.contains("Git commit: required paths passed"));
        assert!(brief.contains("mcp__agent_run_worker__finish"));
        assert!(brief.contains(&format!("TMPDIR: {scratch:?}")));

        let custom = check.workdir.join("custom");
        fs::create_dir(&custom).unwrap();
        check.tmpdir = Some(custom.clone());
        check
            .prepare(None)
            .unwrap()
            .install_environment(&check, &mut environment, &TEMP_NAMES);
        assert_eq!(environment["TMPDIR"], custom.to_str().unwrap());
    }

    /// An unsafe scratch entry yields a warning and its link target stays untouched.
    #[test]
    fn scratch_conflict_is_advisory() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let check = plan(root.path());
        symlink(outside.path(), check.workdir.join(".scratch")).unwrap();
        let facts = check.prepare(None).unwrap();
        assert!(facts.tmpdir.is_none());
        assert!(
            facts
                .warnings
                .iter()
                .any(|warning| warning.contains("repair .scratch"))
        );
        assert!(fs::read_dir(outside.path()).unwrap().next().is_none());
    }

    /// Outside, missing and symlink paths receive the same path-free refusal.
    #[test]
    fn system_scope_refusals_do_not_disclose_outside_path_facts() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside = outside.path().canonicalize().unwrap();
        let mut check = plan(root.path());
        let missing = outside.join("missing");
        let alias = check.workdir.join("alias");
        symlink(&outside, &alias).unwrap();
        let refusal = invalid(SCOPE_REFUSAL).to_string();
        for path in [&outside, &missing, &alias, &alias.join("missing")] {
            check.writes = vec![path.clone()];
            assert_eq!(check.system_warnings().unwrap_err().to_string(), refusal);
            check.writes.clear();
            check.tmpdir = Some(path.clone());
            assert_eq!(check.system_warnings().unwrap_err().to_string(), refusal);
            check.tmpdir = None;
        }
        let endpoint = outside.join("live.sock");
        let listener = UnixListener::bind(&endpoint).unwrap();
        for path in [endpoint, missing, alias.join("live.sock")] {
            check.sockets = vec![path];
            assert_eq!(check.system_warnings().unwrap_err().to_string(), refusal);
        }
        listener.set_nonblocking(true).unwrap();
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    /// An in-scope socket can be reached through its pinned descriptor.
    #[cfg(target_os = "linux")]
    #[test]
    fn system_scope_connects_to_a_granted_socket() {
        let root = tempfile::tempdir().unwrap();
        let mut check = plan(root.path());
        let endpoint = check.workdir.join("live.sock");
        let listener = UnixListener::bind(&endpoint).unwrap();
        check.sockets.push(endpoint);
        check.system_warnings().unwrap();
        listener.set_nonblocking(true).unwrap();
        listener.accept().unwrap();
    }

    /// An endpoint inside the workdir can be blocked by native policy even
    /// when the worker accepts the ordinary launch with no endpoint required.
    #[test]
    fn system_scope_distinguishes_shell_policy_from_outside_scope() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let mut check = plan(root.path());
        check.write_workdir = false;
        check.writable.clear();
        check.shell = false;
        check.system_warnings().unwrap();

        let path = check.workdir.join("absent.sock");
        check.sockets.push(path.clone());
        let refusal = check.system_warnings().unwrap_err().to_string();
        assert!(refusal.contains("native shell/network grant"));
        assert!(!refusal.contains(path.to_str().unwrap()));

        check.sockets = vec![outside.path().join("absent.sock")];
        assert_eq!(
            check.system_warnings().unwrap_err().to_string(),
            SCOPE_REFUSAL
        );
    }

    /// Replacing workspace scratch after admission cannot affect the independent
    /// run temp path. An explicit symlink and an unsafe ancestor are refused.
    #[test]
    fn system_temp_uses_a_separate_private_leaf_after_workspace_swap() {
        let root = tempfile::tempdir().unwrap();
        let worker = tempfile::tempdir().unwrap();
        let worker = worker.path().canonicalize().unwrap();
        let mut check = plan(root.path());
        let scratch = check.workdir.join(".scratch");
        fs::create_dir(&scratch).unwrap();
        check.system_warnings().unwrap();
        fs::rename(&scratch, check.workdir.join("former-scratch")).unwrap();
        symlink(&worker, &scratch).unwrap();
        let id = AgentId::new();
        // Exercise the same creation path even on a test runner whose
        // externally provisioned TMPDIR ancestors are shared or UID-mapped.
        let parent = Dir::open_searchable(&worker).unwrap();
        let temp = create_run_temp(&worker, &parent, &id).unwrap();
        assert_eq!(fs::metadata(&temp).unwrap().mode() & 0o777, 0o700);
        assert_eq!(check.inspect(true).unwrap().tmpdir, None);
        assert_ne!(temp, scratch.canonicalize().unwrap());
        if Dir::open_protected(&worker).is_ok() {
            assert_eq!(
                check.prepare(Some((&worker, &id))).unwrap().tmpdir,
                Some(temp)
            );
        }
        check.tmpdir = Some(scratch);
        assert_eq!(
            check.system_warnings().unwrap_err().to_string(),
            invalid(SCOPE_REFUSAL).to_string()
        );
        let shared = check.workdir.join("shared");
        let explicit = shared.join("private");
        fs::create_dir_all(&explicit).unwrap();
        fs::set_permissions(&shared, fs::Permissions::from_mode(0o770)).unwrap();
        check.tmpdir = Some(explicit.clone());
        assert!(Dir::open_protected(&explicit).is_err());
        assert_eq!(
            check.system_warnings().unwrap_err().to_string(),
            invalid(TEMP_REFUSAL).to_string()
        );
    }

    /// A shared base needs no common directory owned by a particular worker.
    #[test]
    fn system_temp_creates_run_leaves_directly_in_a_shared_sticky_base() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path();
        fs::set_permissions(base, fs::Permissions::from_mode(0o1777)).unwrap();
        let old_stem = base.join(".agent-run-tmp");
        fs::create_dir(&old_stem).unwrap();
        fs::set_permissions(&old_stem, fs::Permissions::from_mode(0o500)).unwrap();
        let dir = Dir::open(base).unwrap();
        let first = create_run_temp(base, &dir, &AgentId::new()).unwrap();
        let second = create_run_temp(base, &dir, &AgentId::new()).unwrap();
        assert_ne!(first, second);
        for leaf in [first, second] {
            assert_eq!(leaf.parent(), Some(base));
            assert_eq!(fs::metadata(leaf).unwrap().mode() & 0o777, 0o700);
        }
        assert_eq!(fs::read_dir(old_stem).unwrap().count(), 0);
    }

    /// Index and objects must be writable, and only the deepest existing ref
    /// directory needs to accept creation of the next entry.
    #[test]
    fn git_commit_requires_writable_object_and_ref_paths() {
        let root = tempfile::tempdir().unwrap();
        let mut check = plan(root.path());
        let git = check.workdir.join(".git");
        git_fixture(&git, &git);
        check.needs_git_commit = true;
        assert!(check.system_warnings().is_ok());
        fs::write(git.join("index.lock"), "").unwrap();
        assert!(
            check
                .system_warnings()
                .unwrap_err()
                .to_string()
                .contains("index.lock")
        );
        fs::remove_file(git.join("index.lock")).unwrap();
        // Root can write these modes, so exercise permission refusals as an ordinary UID.
        // SAFETY: read-only process credential lookup.
        if unsafe { libc::geteuid() } != 0 {
            for (path, reason) in [
                ("objects", "objects"),
                ("refs/heads", "HEAD reference directory"),
            ] {
                let path = git.join(path);
                fs::set_permissions(&path, fs::Permissions::from_mode(0o500)).unwrap();
                let refusal = check.system_warnings().unwrap_err().to_string();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
                assert!(refusal.contains(reason), "{refusal}");
            }
            let refs = git.join("refs");
            fs::set_permissions(&refs, fs::Permissions::from_mode(0o500)).unwrap();
            // A missing directory under writable refs/heads can be created.
            fs::write(git.join("HEAD"), "ref: refs/heads/team/topic\n").unwrap();
            let writable_heads = check.system_warnings();
            // A missing directory directly under read-only refs cannot.
            fs::write(git.join("HEAD"), "ref: refs/team/topic\n").unwrap();
            let readonly_refs = check.system_warnings();
            fs::set_permissions(&refs, fs::Permissions::from_mode(0o700)).unwrap();
            writable_heads.unwrap();
            assert!(
                readonly_refs
                    .unwrap_err()
                    .to_string()
                    .contains("HEAD reference directory")
            );
        }
        assert!(check.system_warnings().is_ok());
        fs::rename(git.join("refs"), git.join("unused-refs")).unwrap();
        assert!(
            check
                .system_warnings()
                .unwrap_err()
                .to_string()
                .contains("HEAD reference directory")
        );
    }

    /// A configured TMP or TEMP only overrides that particular default.
    #[test]
    fn temp_defaults_merge_independently_and_brief_tracks_each_entry() {
        let root = tempfile::tempdir().unwrap();
        let check = plan(root.path());
        let facts = check.prepare(None).unwrap();
        let scratch = check.workdir.join(".scratch");
        for name in ["TMP", "TEMP"] {
            let mut env = BTreeMap::from([
                ("TMPDIR".into(), "/inherited".into()),
                (name.into(), "/harness".into()),
            ]);
            let entries = facts.install_environment(&check, &mut env, &[name]);
            let brief = check.brief(&facts, false, &entries, HarnessId::ClaudeCode);
            for variable in TEMP_NAMES {
                if variable == name {
                    assert_eq!(env[name], "/harness");
                    assert!(brief.contains(&format!("{name}: harness environment override")));
                } else {
                    assert_eq!(env[variable], scratch.to_str().unwrap());
                    assert!(brief.contains(&format!("{variable}: {scratch:?}")));
                }
            }
        }
    }
}
