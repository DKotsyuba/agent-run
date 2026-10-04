//! Read-only shared asset roots behind a native launch sandbox.
//!
//! The target storage model keeps one central immutable payload store and
//! exposes it to each private home through symlinks. Same-UID file
//! permissions cannot enforce that model, because the launched child owns
//! the same UID and can rewrite, chmod, unlink or rename the shared bytes.
//! This module proves the smallest launch guard for qualified hosts.
//!
//! On macOS [`SharedAssetGuard::wrap`] rewrites a plain `program args…` argv
//! into `sandbox-exec -p <profile> -D ROOT=<canonical> … -- program args…`.
//! The profile denies every write-shaped operation (`file-write*`, which
//! covers open-for-write, create, unlink, rename, link, chmod and chown on
//! this OS) whose path is the canonical shared root or anything beneath it,
//! and denies those operations on each ancestor directory of the root as one
//! exact path, so a writable ancestor cannot be renamed to move the root out
//! from under the deny rule. Every filesystem path travels only in `-D`
//! parameter arguments — never embedded in the profile text — and ancestor
//! parameters are escaped as Seatbelt regex literals, so quoted or
//! metacharacter paths cannot widen or redirect the rules.
//!
//! On Linux the same rewrite produces `bwrap --unshare-user --die-with-parent
//! --dev-bind / / --bind <ancestor> <ancestor>… --ro-bind <root> <root> --
//! program args…` (bubblewrap at [`LINUX_BWRAP`]). The child sees the host
//! filesystem unchanged except that the root and every mount beneath it are
//! a read-only bind, so write, create, truncate, unlink, rename, link,
//! chmod, chown and timestamp changes fail with `EROFS` and a hard link out
//! of the tree fails with `EXDEV`. Each ancestor directory below `/` is
//! bound onto itself (still writable), which makes it a mount point in the
//! child's namespace: renaming or removing the root or any ancestor fails
//! with `EBUSY`. The fresh user namespace keeps those mounts locked against
//! nested namespaces (a descendant cannot unmount them or clear read-only)
//! and puts every unconfined same-UID process in a parent namespace, so
//! `/proc/<pid>/root` and `/proc/<pid>/cwd` links cannot reach the writable
//! host view. No PID, network, IPC or session namespace is added and no
//! environment variable is touched: PIDs, the caller's process group,
//! signals, stdio, cwd and exit status stay as without the wrapper.
//! Setuid executables cannot gain privilege inside the user namespace.
//! Before launch the wrapper refuses a root that another mount of the same
//! filesystem exposes at a second path the bind would not cover.
//!
//! On both platforms reads of shared assets through private-home symlinks
//! and ordinary writes inside the child's own workdir stay allowed; the
//! wrapper only adds denials and never loosens any other constraint. The
//! sandbox is inherited by every descendant of the wrapped child. Before
//! launch, a bounded no-follow inode scan refuses external hardlink aliases
//! while permitting links wholly inside the shared tree. Every other
//! operating system, and a host lacking the native helper, is an explicit
//! [`SharedAssetGuardError::UnsupportedPlatform`] refusal, never a fallback.
//! Whether the kernel actually lets the helper apply its namespace is proven
//! by the caller's qualification probe, which runs a real guarded child.
//!
//! This module owns only the verified transformation; adapters apply it to
//! their launch plans. It never claims protection over unrelated
//! pre-existing processes (for example MCP servers started elsewhere, or a
//! same-UID service the child asks to act on its behalf): only the wrapped
//! child and its descendants are constrained.
use std::{
    collections::HashMap,
    ffi::OsString,
    fmt, io,
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
};

/// Absolute path of the native Seatbelt wrapper used on qualified macOS hosts.
pub const MACOS_SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

/// Absolute path of the bubblewrap launcher used on qualified Linux hosts.
/// Only this fixed distribution path is trusted; a `PATH` lookup could be
/// redirected by the same user the guard constrains.
pub const LINUX_BWRAP: &str = "/usr/bin/bwrap";

/// Maximum number of store entries examined before launch refuses the tree:
/// the shared-tree entry bound grew fourfold for native curated mirrors, so
/// the launch scan keeps the same headroom ratio.
const MAX_SCAN_PATHS: usize = 400_000;

/// Profile parameter naming the canonical shared root inside [`SharedAssetGuard::profile`].
const ROOT_PARAM: &str = "ROOT";

/// Why one shared asset root cannot be guarded, or why no guard exists.
#[derive(Debug)]
pub enum SharedAssetGuardError {
    /// The host offers no native launch sandbox this guard can use. The
    /// payload names the exact missing capability; callers must fail the
    /// launch rather than fall back to a private copy.
    UnsupportedPlatform(&'static str),
    /// The root was not an absolute path, so it cannot name one canonical
    /// store location.
    RootNotAbsolute(PathBuf),
    /// The root, or one of its parent directories, is a symbolic link. The
    /// payload `component` is the offending path prefix. Substituted parents
    /// are refused because Seatbelt filters match canonical paths and a
    /// substituted component could redirect the protected subtree.
    RootSymlinkComponent { root: PathBuf, component: PathBuf },
    /// The root, or one of its required parent directories, exists but is not
    /// a real directory.
    RootNotDirectory(PathBuf),
    /// The root as written does not equal its canonical form, for example it
    /// contains `.` or `..` components. `canonical` is the resolved form the
    /// caller must use instead.
    RootNotCanonical { given: PathBuf, canonical: PathBuf },
    /// The root is not valid UTF-8, so it cannot be named in a `-D` profile
    /// parameter.
    UnsafeCharacters(PathBuf),
    /// A regular file has an alias outside the root. `links` is its total
    /// link count; every link must be found inside the root before launch.
    AliasedAsset { path: PathBuf, links: u64 },
    /// Another mount of the root's filesystem, at the payload mount point,
    /// exposes the root or part of it at a second path that a read-only
    /// bind of the canonical root would leave writable (Linux only).
    AliasedMount(PathBuf),
    /// The store contains too many paths to verify within the fixed scan
    /// budget; reduce the tree before launching.
    TooManyAssets,
    /// A filesystem operation failed while validating the root or walking its
    /// contents.
    Io(io::Error),
}

impl fmt::Display for SharedAssetGuardError {
    /// Renders one line naming the refused root or missing capability.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPlatform(why) => {
                write!(f, "no native shared-asset sandbox: {why}")
            }
            Self::RootNotAbsolute(root) => {
                write!(f, "shared root {root:?} is not an absolute path")
            }
            Self::RootSymlinkComponent { root, component } => write!(
                f,
                "shared root {root:?} passes through symbolic link {component:?}"
            ),
            Self::RootNotDirectory(path) => {
                write!(f, "shared root component {path:?} is not a real directory")
            }
            Self::RootNotCanonical { given, canonical } => write!(
                f,
                "shared root {given:?} is not canonical; use {canonical:?}"
            ),
            Self::UnsafeCharacters(root) => {
                write!(f, "shared root {root:?} is not valid UTF-8 text")
            }
            Self::AliasedAsset { path, links } => write!(
                f,
                "shared asset {path:?} has {links} hard links, including an outside alias"
            ),
            Self::AliasedMount(point) => write!(
                f,
                "shared root is also reachable through the mount at {point:?}"
            ),
            Self::TooManyAssets => write!(f, "shared asset scan exceeds {MAX_SCAN_PATHS} paths"),
            Self::Io(error) => write!(f, "shared root check failed: {error}"),
        }
    }
}

impl std::error::Error for SharedAssetGuardError {
    /// Names [`Self::Io`]'s inner error as its source; other variants are
    /// self-contained launch refusals.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for SharedAssetGuardError {
    /// Wraps one filesystem failure observed while validating the root.
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// A validated launch guard for one canonical shared asset root.
///
/// Construction validates the root only; [`Self::wrap`] performs the native
/// argv transformation, and [`Self::profile`] plus [`Self::parameters`]
/// expose the exact Seatbelt text and `-D` bindings that transformation
/// uses, so tests and reviews can verify the rules.
#[derive(Debug, Clone)]
pub struct SharedAssetGuard {
    /// Canonical root directory, validated to have no symlink components.
    root: PathBuf,
    /// Canonical root text, guaranteed UTF-8 by construction.
    root_text: String,
}

impl SharedAssetGuard {
    /// Validates one shared asset root for guarding.
    ///
    /// `root` must be absolute, valid UTF-8, must not itself be a symbolic
    /// link nor pass through any symlinked or substituted parent directory,
    /// every component must be a real directory, and the path as written
    /// must equal its canonical form (no `.`/`..` components). On success
    /// the guard owns the canonical text. No filesystem mutation occurs.
    /// Missing paths and I/O failures return [`SharedAssetGuardError::Io`];
    /// every rule violation returns its named variant. Validation is
    /// platform-independent so fixture construction stays testable
    /// everywhere.
    pub fn new(root: &Path) -> Result<Self, SharedAssetGuardError> {
        if !root.is_absolute() {
            return Err(SharedAssetGuardError::RootNotAbsolute(root.to_path_buf()));
        }
        let root_text = match root.to_str() {
            Some(text) => text.to_string(),
            None => return Err(SharedAssetGuardError::UnsafeCharacters(root.to_path_buf())),
        };
        let canonical = root.canonicalize().map_err(SharedAssetGuardError::Io)?;
        let mut prefix = PathBuf::new();
        for component in root.components() {
            match component {
                Component::RootDir => prefix.push("/"),
                Component::Normal(name) => prefix.push(name),
                // Unreachable after the canonical comparison; kept exhaustive.
                Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                    return Err(SharedAssetGuardError::RootNotCanonical {
                        given: root.to_path_buf(),
                        canonical: canonical.clone(),
                    });
                }
            }
            let status = prefix
                .symlink_metadata()
                .map_err(SharedAssetGuardError::Io)?;
            if status.file_type().is_symlink() {
                return Err(SharedAssetGuardError::RootSymlinkComponent {
                    root: root.to_path_buf(),
                    component: prefix.clone(),
                });
            }
            if !status.is_dir() {
                return Err(SharedAssetGuardError::RootNotDirectory(prefix));
            }
        }
        if canonical != root {
            return Err(SharedAssetGuardError::RootNotCanonical {
                given: root.to_path_buf(),
                canonical,
            });
        }
        Ok(Self {
            root: root.to_path_buf(),
            root_text,
        })
    }

    /// The validated canonical shared root this guard protects.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The exact canonical root text carried by [`Self::parameters`].
    pub fn root_text(&self) -> &str {
        &self.root_text
    }

    /// Renders the parameterized Seatbelt profile `wrap` embeds.
    ///
    /// The profile text contains no filesystem path: it is
    /// `(version 1)(allow default)`, one `(deny file-write* (subpath (param
    /// "ROOT")))` rule protecting the root and everything beneath it, and one
    /// `(deny file-write* (regex (param "ANC<n>")))` rule per ancestor
    /// directory matched through the exact anchored value bound in
    /// [`Self::parameters`], blocking rename, unlink and chmod of the root
    /// and its ancestors while leaving every other operation — including
    /// ordinary workdir writes — allowed. The text depends only on the
    /// root's depth, never on its spelling.
    pub fn profile(&self) -> String {
        let mut profile = format!(
            "(version 1)(allow default)(deny file-write* (subpath (param \"{ROOT_PARAM}\")))"
        );
        for index in 0..self.root.ancestors().count().saturating_sub(1) {
            profile.push_str(&format!(
                "(deny file-write* (regex (param \"ANC{index}\")))"
            ));
        }
        profile
    }

    /// Builds the `-D` parameter bindings for [`Self::profile`], in argv
    /// `NAME=value` form.
    ///
    /// Binds `ROOT` to the canonical root text, then `ANC0…ANCn` to each
    /// ancestor directory of the root (nearest first, ending at `/`) as
    /// `^<escaped path>$` Seatbelt regex literals. Anchors and regex
    /// metacharacter escaping make each rule match exactly that directory
    /// and nothing beneath or beside it, so a lookalike directory name is
    /// never denied. Callers pass each entry as one argv element after
    /// `-D`, never through a shell.
    pub fn parameters(&self) -> Vec<(String, String)> {
        let mut parameters = vec![(ROOT_PARAM.to_string(), self.root_text.clone())];
        for (index, ancestor) in self.root.ancestors().skip(1).enumerate() {
            let text = ancestor.to_str().unwrap_or_default();
            parameters.push((
                format!("ANC{index}"),
                format!("^{}$", escape_seatbelt_regex(text)),
            ));
        }
        parameters
    }

    /// Rewrites one launch argv into its guarded form.
    ///
    /// Given the intended `program` and its `args`, returns the native
    /// wrapper argv ready for the spawn call a `LaunchPlan` already drives;
    /// `argv[0]` is the absolute wrapper executable and the original program
    /// and arguments follow `--` unchanged. On macOS that is
    /// `sandbox-exec -p <profile> -D <parameters>… -- program args…` after
    /// verifying `/usr/bin/sandbox-exec` is an executable file. On Linux it is
    /// [`Self::bubblewrap_argv`] after verifying [`LINUX_BWRAP`] is an
    /// executable file and that [`Self::verify_no_mount_aliases`] passes. Both
    /// require [`Self::verify_no_hardlink_aliases`], so a launch cannot
    /// proceed while an alias could bypass the guard. A missing helper is
    /// [`SharedAssetGuardError::UnsupportedPlatform`] naming it; other
    /// operating systems always return that variant. Callers must refuse the
    /// shared launch and never fall back to copying assets into private
    /// homes. This builds argv only: whether the kernel admits the Linux
    /// namespaces is proven when the caller runs a guarded child. No process
    /// is spawned and no environment or approval behavior changes.
    pub fn wrap(
        &self,
        program: &Path,
        args: &[String],
    ) -> Result<Vec<OsString>, SharedAssetGuardError> {
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::fs::PermissionsExt;
            let executable = match std::fs::metadata(MACOS_SANDBOX_EXEC) {
                Ok(metadata) => metadata,
                Err(_) => {
                    return Err(SharedAssetGuardError::UnsupportedPlatform(
                        "sandbox-exec is not installed at /usr/bin/sandbox-exec",
                    ))
                }
            };
            if !executable.is_file() || executable.permissions().mode() & 0o111 == 0 {
                return Err(SharedAssetGuardError::UnsupportedPlatform(
                    "sandbox-exec is not an executable file",
                ));
            }
            self.verify_no_hardlink_aliases()?;
            let mut argv = vec![
                OsString::from(MACOS_SANDBOX_EXEC),
                OsString::from("-p"),
                OsString::from(self.profile()),
            ];
            for (name, value) in self.parameters() {
                argv.push(OsString::from("-D"));
                argv.push(OsString::from(format!("{name}={value}")));
            }
            argv.push(OsString::from("--"));
            argv.push(program.as_os_str().to_os_string());
            argv.extend(args.iter().map(OsString::from));
            Ok(argv)
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::PermissionsExt;
            let executable = std::fs::metadata(LINUX_BWRAP).map_err(|_| {
                SharedAssetGuardError::UnsupportedPlatform(
                    "bubblewrap is not installed at /usr/bin/bwrap",
                )
            })?;
            if !executable.is_file() || executable.permissions().mode() & 0o111 == 0 {
                return Err(SharedAssetGuardError::UnsupportedPlatform(
                    "/usr/bin/bwrap is not an executable file",
                ));
            }
            self.verify_no_hardlink_aliases()?;
            self.verify_no_mount_aliases()?;
            Ok(self.bubblewrap_argv(program, args))
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let _ = (program, args);
            Err(SharedAssetGuardError::UnsupportedPlatform(
                "no native shared-asset launch sandbox exists for this operating system; \
                 refusing to fake a guard",
            ))
        }
    }

    /// Builds the bubblewrap argv [`Self::wrap`] returns on Linux.
    ///
    /// Order is significant because bubblewrap applies mounts in sequence:
    /// `--unshare-user` always creates a fresh user namespace (also for a
    /// setuid helper or a root caller), `--die-with-parent` ties the child to
    /// the wrapper, `--dev-bind / /` keeps the whole host view including
    /// devices, each ancestor of the root below `/` is then bound onto itself
    /// from the outermost inward, and the root is bound read-only last so no
    /// later bind can shadow it. Paths are passed as single argv elements,
    /// never through a shell; the program and its arguments follow `--`
    /// unchanged. Pure: no filesystem access and no validation beyond
    /// construction.
    #[cfg(any(target_os = "linux", test))]
    fn bubblewrap_argv(&self, program: &Path, args: &[String]) -> Vec<OsString> {
        let mut argv: Vec<OsString> = [
            LINUX_BWRAP,
            "--unshare-user",
            "--die-with-parent",
            "--dev-bind",
            "/",
            "/",
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        let mut ancestors: Vec<&Path> = self
            .root
            .ancestors()
            .skip(1)
            .filter(|ancestor| ancestor.parent().is_some())
            .collect();
        ancestors.reverse();
        for ancestor in ancestors {
            argv.push(OsString::from("--bind"));
            argv.push(ancestor.as_os_str().to_os_string());
            argv.push(ancestor.as_os_str().to_os_string());
        }
        argv.push(OsString::from("--ro-bind"));
        argv.push(self.root.as_os_str().to_os_string());
        argv.push(self.root.as_os_str().to_os_string());
        argv.push(OsString::from("--"));
        argv.push(program.as_os_str().to_os_string());
        argv.extend(args.iter().map(OsString::from));
        argv
    }

    /// Fails when another mount of the root's filesystem exposes the root, or
    /// part of it, at a second path the read-only bind would not cover.
    ///
    /// The root's own mount is identified exactly by the `mnt_id` the kernel
    /// reports for an open descriptor of the root, then
    /// `/proc/self/mountinfo` is classified by [`mount_alias`]. A mount table
    /// that does not list the root's mount is
    /// [`SharedAssetGuardError::UnsupportedPlatform`]; an alias is
    /// [`SharedAssetGuardError::AliasedMount`] naming its mount point; read
    /// failures are [`SharedAssetGuardError::Io`]. Like the hardlink scan
    /// this reflects launch time only: a mount added later by an unrelated
    /// process is outside the guarantee.
    #[cfg(target_os = "linux")]
    pub fn verify_no_mount_aliases(&self) -> Result<(), SharedAssetGuardError> {
        use std::os::fd::AsRawFd;
        let directory = std::fs::File::open(&self.root)?;
        let info = std::fs::read_to_string(format!("/proc/self/fdinfo/{}", directory.as_raw_fd()))?;
        let mount_id = info
            .lines()
            .find_map(|line| line.strip_prefix("mnt_id:"))
            .and_then(|value| value.trim().parse::<u64>().ok())
            .ok_or(SharedAssetGuardError::UnsupportedPlatform(
                "the kernel does not report the shared root's mount id",
            ))?;
        let table = std::fs::read_to_string("/proc/self/mountinfo")?;
        match mount_alias(&table, mount_id, &self.root) {
            Some(Ok(())) => Ok(()),
            Some(Err(point)) => Err(SharedAssetGuardError::AliasedMount(point)),
            None => Err(SharedAssetGuardError::UnsupportedPlatform(
                "the shared root's mount is missing from /proc/self/mountinfo",
            )),
        }
    }

    /// Fails if any regular file has a hard link outside the root.
    ///
    /// Counts paths by `(device, inode)` without following symlinks. Internal
    /// hard links are allowed only when their count matches `nlink`; a larger
    /// count means an external writable alias could bypass path denials.
    /// The scan is capped at [`MAX_SCAN_PATHS`] (400000) entries and fails
    /// closed beyond that.
    /// [`Self::wrap`] runs this check before launch. An unrelated same-UID
    /// process adding an alias after the scan remains outside this guarantee.
    pub fn verify_no_hardlink_aliases(&self) -> Result<(), SharedAssetGuardError> {
        self.verify_hardlink_aliases_bounded(MAX_SCAN_PATHS)
    }

    /// Scans at most `limit` entries, refusing a larger tree before launch.
    /// The scan and inode counts have the same semantics as the public check.
    fn verify_hardlink_aliases_bounded(&self, limit: usize) -> Result<(), SharedAssetGuardError> {
        let mut directories = vec![self.root.clone()];
        let mut files: HashMap<(u64, u64), (PathBuf, u64, u64)> = HashMap::new();
        let mut scanned = 0;
        while let Some(dir) = directories.pop() {
            for entry in std::fs::read_dir(dir)? {
                let path = entry?.path();
                scanned += 1;
                if scanned > limit {
                    return Err(SharedAssetGuardError::TooManyAssets);
                }
                let status = path.symlink_metadata()?;
                if status.is_dir() {
                    directories.push(path);
                } else if status.is_file() {
                    let file = files.entry((status.dev(), status.ino())).or_insert((
                        path,
                        status.nlink(),
                        0,
                    ));
                    file.2 += 1;
                }
            }
        }
        for (_, (path, links, found)) in files {
            if found != links {
                return Err(SharedAssetGuardError::AliasedAsset { path, links });
            }
        }
        Ok(())
    }
}

/// Escapes one path text for use as a Seatbelt regex parameter value.
///
/// Ancestor rules match through the regex engine, so every regex
/// metacharacter is backslash-escaped and the caller anchors the result with
/// `^…$` to match exactly one directory. Escaping was verified to survive
/// `-D` parameter passing on macOS without shell or profile quoting.
fn escape_seatbelt_regex(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        if matches!(
            character,
            '^' | '$' | '.' | '|' | '?' | '*' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '\\'
        ) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

/// Classifies one `/proc/self/mountinfo` table for
/// [`SharedAssetGuard::verify_no_mount_aliases`].
///
/// `table` is the raw mountinfo text, `mount_id` the id of the mount that
/// path lookup of `root` reaches, and `root` the canonical shared root.
/// Every mount of the same device (`major:minor`, shared by bind mounts and
/// subvolumes of one filesystem) is compared through its filesystem subtree
/// field. A mount whose subtree contains the root's subtree is an alias
/// unless it maps the root back onto `root` itself (the root's own mount and
/// ancestor binds stacked beneath it). A mount whose subtree lies inside the
/// root's subtree is an alias unless it is mounted at the matching position
/// beneath `root`, where the recursive read-only bind covers it. Returns
/// `None` when no line carries `mount_id` or `root` does not lie beneath
/// that mount's point, `Some(Err(mount point))` for the first alias, and
/// `Some(Ok(()))` otherwise. Malformed lines are skipped and fields are
/// decoded from mountinfo's octal escapes. Pure: no filesystem access.
#[cfg(any(target_os = "linux", test))]
fn mount_alias(table: &str, mount_id: u64, root: &Path) -> Option<Result<(), PathBuf>> {
    let mounts: Vec<(u64, &str, PathBuf, PathBuf)> = table
        .lines()
        .filter_map(|line| {
            let mut fields = line.split(' ');
            let id = fields.next()?.parse().ok()?;
            let device = fields.nth(1)?;
            let tree = unescape_mountinfo(fields.next()?);
            let point = unescape_mountinfo(fields.next()?);
            Some((id, device, tree, point))
        })
        .collect();
    let (_, device, tree, point) = mounts.iter().find(|mount| mount.0 == mount_id)?;
    let inner = tree.join(root.strip_prefix(point).ok()?);
    for (_, other_device, other_tree, other_point) in &mounts {
        if other_device != device {
            continue;
        }
        let aliased = if let Ok(rest) = inner.strip_prefix(other_tree) {
            other_point.join(rest) != root
        } else if let Ok(rest) = other_tree.strip_prefix(&inner) {
            *other_point != root.join(rest)
        } else {
            false
        };
        if aliased {
            return Some(Err(other_point.clone()));
        }
    }
    Some(Ok(()))
}

/// Decodes one mountinfo path field, whose space, tab, newline and backslash
/// bytes the kernel writes as three-digit octal escapes (`\040`). Any other
/// byte, including a backslash not followed by three octal digits, is kept
/// verbatim, so the result is the exact non-UTF-8-safe path.
#[cfg(any(target_os = "linux", test))]
fn unescape_mountinfo(field: &str) -> PathBuf {
    use std::os::unix::ffi::OsStringExt;
    let bytes = field.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let escape = bytes.get(index + 1..index + 4).filter(|digits| {
            bytes[index] == b'\\' && digits.iter().all(|digit| (b'0'..=b'7').contains(digit))
        });
        match escape {
            Some(digits) => {
                decoded.push(
                    digits
                        .iter()
                        .fold(0u8, |value, digit| value.wrapping_mul(8) + (digit - b'0')),
                );
                index += 4;
            }
            None => {
                decoded.push(bytes[index]);
                index += 1;
            }
        }
    }
    PathBuf::from(OsString::from_vec(decoded))
}

#[cfg(test)]
/// Pure validation checks and qualified-host Seatbelt and bubblewrap behavior checks.
mod tests {
    use super::*;

    /// Builds a canonical temporary shared root and its writable sibling.
    fn fixture() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().canonicalize().unwrap();
        let root = base.join("shared");
        let work = base.join("work");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&work).unwrap();
        (temp, root, work)
    }

    /// Accepts internal aliases and rejects an existing external hardlink.
    #[test]
    fn hardlink_scan_distinguishes_internal_and_external_aliases() {
        let (_temp, root, work) = fixture();
        let first = root.join("first");
        std::fs::write(&first, b"original").unwrap();
        std::fs::hard_link(&first, root.join("second")).unwrap();
        let guard = SharedAssetGuard::new(&root).unwrap();
        guard.verify_no_hardlink_aliases().unwrap();
        std::fs::hard_link(&first, work.join("outside")).unwrap();
        assert!(matches!(
            guard.verify_no_hardlink_aliases(),
            Err(SharedAssetGuardError::AliasedAsset { .. })
        ));
        #[cfg(target_os = "macos")]
        assert!(matches!(
            guard.wrap(Path::new("/bin/true"), &[]),
            Err(SharedAssetGuardError::AliasedAsset { .. })
        ));
        #[cfg(target_os = "linux")]
        assert!(
            matches!(
                guard.wrap(Path::new("/bin/true"), &[]),
                Err(SharedAssetGuardError::AliasedAsset { .. })
            ) || !Path::new(LINUX_BWRAP).exists()
        );
    }

    /// Refuses trees that exceed the path budget before accepting their files.
    #[test]
    fn hardlink_scan_is_bounded() {
        let (_temp, root, _work) = fixture();
        std::fs::write(root.join("one"), b"x").unwrap();
        std::fs::write(root.join("two"), b"x").unwrap();
        let guard = SharedAssetGuard::new(&root).unwrap();
        assert!(matches!(
            guard.verify_hardlink_aliases_bounded(1),
            Err(SharedAssetGuardError::TooManyAssets)
        ));
    }

    /// Keeps ancestor metacharacters literal in the native regex parameter.
    #[test]
    fn ancestor_parameter_escapes_regex_metacharacters() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().canonicalize().unwrap().join("a[b].c");
        let root = parent.join("shared");
        std::fs::create_dir_all(&root).unwrap();
        let guard = SharedAssetGuard::new(&root).unwrap();
        assert!(guard
            .parameters()
            .iter()
            .any(|(name, value)| { name == "ANC0" && value.ends_with("/a\\[b\\]\\.c$") }));
    }

    /// Keeps paths out of the profile and preserves the intended child argv.
    #[cfg(target_os = "macos")]
    #[test]
    fn wrapper_uses_parameters_and_preserves_child_argv() {
        let (_temp, root, _work) = fixture();
        let guard = SharedAssetGuard::new(&root).unwrap();
        let args = vec!["one".to_string(), "two words".to_string()];
        let argv = guard.wrap(Path::new("/bin/echo"), &args).unwrap();
        assert_eq!(argv[0], MACOS_SANDBOX_EXEC);
        assert_eq!(argv[1], "-p");
        assert!(!guard.profile().contains(root.to_str().unwrap()));
        assert_eq!(argv[2], OsString::from(guard.profile()));
        assert!(argv.windows(2).any(|pair| {
            pair[0] == "-D" && pair[1] == OsString::from(format!("ROOT={}", root.display()))
        }));
        assert_eq!(argv[argv.len() - 4], "--");
        assert_eq!(argv[argv.len() - 3], "/bin/echo");
        assert_eq!(argv[argv.len() - 2], "one");
        assert_eq!(argv[argv.len() - 1], "two words");
    }

    /// Binds every ancestor below `/` outermost first, binds the root
    /// read-only last, and keeps the child argv after `--` unchanged.
    #[test]
    fn bubblewrap_argv_binds_ancestors_then_root_readonly() {
        let (_temp, root, _work) = fixture();
        let guard = SharedAssetGuard::new(&root).unwrap();
        let args = vec!["one".to_string(), "two words".to_string()];
        let argv = guard.bubblewrap_argv(Path::new("/bin/echo"), &args);
        let mut expected: Vec<OsString> = [
            LINUX_BWRAP,
            "--unshare-user",
            "--die-with-parent",
            "--dev-bind",
            "/",
            "/",
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        let mut ancestors: Vec<&Path> = root.ancestors().skip(1).collect();
        ancestors.pop();
        for ancestor in ancestors.into_iter().rev() {
            expected.extend(["--bind".into(), ancestor.into(), ancestor.into()]);
        }
        expected.extend(["--ro-bind".into(), root.clone().into(), root.into()]);
        expected.extend(["--", "/bin/echo", "one", "two words"].map(OsString::from));
        assert_eq!(argv, expected);
    }

    /// Accepts the root's own mount, stacked ancestor binds and covered
    /// submounts, and refuses bind or subvolume views exposing the root
    /// elsewhere, including escaped mount point names.
    #[test]
    fn mount_alias_classifies_same_filesystem_views() {
        let root = Path::new("/home/u/.agent-run/shared-assets/v1");
        let own = "30 1 0:40 /@home /home rw - btrfs /dev/vda rw\n\
                   31 1 0:40 /@ / rw - btrfs /dev/vda rw\n\
                   32 1 0:41 / /tmp rw - tmpfs tmpfs rw\n";
        assert_eq!(mount_alias(own, 30, root), Some(Ok(())));
        assert_eq!(mount_alias(own, 99, root), None);
        assert_eq!(mount_alias(own, 31, root), Some(Ok(())));
        let nested = format!(
            "{own}40 30 0:40 /@home/u /home/u rw - btrfs /dev/vda rw\n\
             41 40 0:40 /@home/u/.agent-run/shared-assets/v1 \
             /home/u/.agent-run/shared-assets/v1 ro - btrfs /dev/vda rw\n\
             42 41 0:40 /@home/u/.agent-run/shared-assets/v1/trees \
             /home/u/.agent-run/shared-assets/v1/trees ro - btrfs /dev/vda rw\n"
        );
        assert_eq!(mount_alias(&nested, 41, root), Some(Ok(())));
        let top = format!("{own}50 1 0:40 / /mnt/top\\040level rw - btrfs /dev/vda rw\n");
        assert_eq!(
            mount_alias(&top, 30, root),
            Some(Err(PathBuf::from("/mnt/top level")))
        );
        let inner = format!(
            "{own}51 1 0:40 /@home/u/.agent-run/shared-assets/v1/trees /srv/trees rw - btrfs /dev/vda rw\n"
        );
        assert_eq!(
            mount_alias(&inner, 30, root),
            Some(Err(PathBuf::from("/srv/trees")))
        );
        assert_eq!(
            unescape_mountinfo("/a\\011b\\134c\\9"),
            PathBuf::from("/a\tb\\c\\9")
        );
    }

    /// A Linux host without bubblewrap is an exact refusal; with it the
    /// guarded argv names the fixed helper and a clean fixture passes the
    /// alias checks.
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_wrap_requires_fixed_bubblewrap() {
        let (_temp, root, _work) = fixture();
        let guard = SharedAssetGuard::new(&root).unwrap();
        match guard.wrap(Path::new("/bin/true"), &[]) {
            Ok(argv) => {
                assert_eq!(argv[0], LINUX_BWRAP);
                guard.verify_no_mount_aliases().unwrap();
            }
            Err(SharedAssetGuardError::UnsupportedPlatform(why)) => {
                assert!(!Path::new(LINUX_BWRAP).exists(), "{why}");
            }
            Err(other) => panic!("{other}"),
        }
    }

    /// Rejects a path that names the same root through a symlinked parent.
    #[test]
    fn root_rejects_symlinked_parent() {
        let (_temp, root, work) = fixture();
        let alias = work.join("alias");
        std::os::unix::fs::symlink(root.parent().unwrap(), &alias).unwrap();
        assert!(matches!(
            SharedAssetGuard::new(&alias.join("shared")),
            Err(SharedAssetGuardError::RootSymlinkComponent { .. })
        ));
    }

    /// Runs one finite shell operation through the guard and returns its status.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    fn run(guard: &SharedAssetGuard, work: &Path, script: &str, args: &[&Path]) -> bool {
        let mut shell_args = vec!["-c".to_string(), script.to_string(), "--".to_string()];
        shell_args.extend(args.iter().map(|path| path.to_str().unwrap().to_string()));
        let argv = guard.wrap(Path::new("/bin/sh"), &shell_args).unwrap();
        std::process::Command::new(&argv[0])
            .args(&argv[1..])
            .current_dir(work)
            .output()
            .unwrap()
            .status
            .success()
    }

    /// Proves denials, permitted sibling work, and inherited child restrictions.
    /// Requires a qualified macOS host that permits applying Seatbelt profiles,
    /// or a Linux host with `/usr/bin/bwrap`, `unshare` and unprivileged user
    /// and mount namespaces; Linux adds namespace-escape and `/proc` alias
    /// checks.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    #[ignore = "requires a qualified host that can apply the native guard (sandbox-exec or bwrap namespaces)"]
    fn live_native_guard() {
        use std::os::unix::fs::PermissionsExt;
        let (_temp, root, work) = fixture();
        let file = root.join("asset");
        std::fs::write(&file, b"original").unwrap();
        let guard = SharedAssetGuard::new(&root).unwrap();
        let sibling = work.join("sibling");
        assert!(run(&guard, &work, "printf ok > \"$1\"", &[&sibling]));
        assert_eq!(std::fs::read(&sibling).unwrap(), b"ok");

        let link = work.join("asset-link");
        std::os::unix::fs::symlink(&file, &link).unwrap();
        let readback = work.join("readback");
        assert!(run(
            &guard,
            &work,
            "cat \"$1\" > \"$2\"",
            &[&link, &readback]
        ));
        assert_eq!(std::fs::read(&readback).unwrap(), b"original");
        assert!(!run(&guard, &work, "printf bad > \"$1\"", &[&file]));
        assert!(!run(&guard, &work, "printf bad > \"$1\"", &[&link]));
        assert!(!run(&guard, &work, "chmod 600 \"$1\"", &[&file]));
        assert_eq!(std::fs::read(&file).unwrap(), b"original");

        let moved = work.join("moved");
        assert!(run(&guard, &work, "chmod 600 \"$1\"", &[&sibling]));
        assert!(run(&guard, &work, "ln \"$1\" \"$2\"", &[&sibling, &moved]));
        assert!(run(&guard, &work, "rm \"$1\"", &[&moved]));
        assert!(run(&guard, &work, "mv \"$1\" \"$2\"", &[&sibling, &moved]));
        assert!(run(&guard, &work, "mv \"$1\" \"$2\"", &[&moved, &sibling]));
        assert!(!run(&guard, &work, "mv \"$1\" \"$2\"", &[&file, &moved]));
        // Linux refuses the rename out of the read-only bind with `EXDEV`;
        // `mv` then copies (a permitted read) and its unlink is refused, so
        // the shared inode stays in place and only an unrelated copy remains.
        #[cfg(target_os = "linux")]
        if moved.exists() {
            assert_ne!(
                std::fs::metadata(&moved).unwrap().ino(),
                std::fs::metadata(&file).unwrap().ino()
            );
            std::fs::remove_file(&moved).unwrap();
        }
        assert!(!run(&guard, &work, "rm \"$1\"", &[&file]));
        assert!(!run(&guard, &work, "ln \"$1\" \"$2\"", &[&file, &moved]));
        assert!(!moved.exists());
        let empty = root.join("empty");
        std::fs::create_dir(&empty).unwrap();
        let work_empty = work.join("empty");
        std::fs::create_dir(&work_empty).unwrap();
        assert!(run(&guard, &work, "rmdir \"$1\"", &[&work_empty]));
        assert!(!run(&guard, &work, "rmdir \"$1\"", &[&empty]));
        assert!(!run(&guard, &work, "mv \"$1\" \"$2\"", &[&root, &moved]));
        let base = root.parent().unwrap();
        let outside = base.with_extension("moved");
        assert!(!run(&guard, &work, "mv \"$1\" \"$2\"", &[base, &outside]));
        assert!(root.exists());

        // The guard, not directory modes, is the same-UID immutability
        // boundary: a published owner-only `0o700` shared-tree directory is
        // writable by mode, yet a guarded child can neither create inside
        // it, replace its payload, nor chmod the payload.
        let tree = root.join("tree");
        std::fs::create_dir(&tree).unwrap();
        std::fs::set_permissions(&tree, std::fs::Permissions::from_mode(0o700)).unwrap();
        let payload = tree.join("payload");
        std::fs::write(&payload, b"payload").unwrap();
        assert!(!run(
            &guard,
            &work,
            "printf bad > \"$1\"",
            &[&tree.join("new")]
        ));
        let replacement = work.join("replacement");
        std::fs::write(&replacement, b"replacement").unwrap();
        assert!(!run(
            &guard,
            &work,
            "mv \"$1\" \"$2\"",
            &[&replacement, &payload]
        ));
        assert_eq!(std::fs::read(&replacement).unwrap(), b"replacement");
        assert!(!run(&guard, &work, "chmod 600 \"$1\"", &[&payload]));
        assert_eq!(std::fs::read(&payload).unwrap(), b"payload");
        assert!(!tree.join("new").exists());

        let child_script = work.join("child.sh");
        std::fs::write(&child_script, "printf bad > \"$1\"\n").unwrap();
        assert!(run(
            &guard,
            &work,
            "sh \"$2\" \"$1\"",
            &[&sibling, &child_script]
        ));
        assert!(!run(
            &guard,
            &work,
            "sh \"$2\" \"$1\"",
            &[&file, &child_script]
        ));
        let grandchild_script = work.join("grandchild.sh");
        std::fs::write(&grandchild_script, "sh \"$2\" \"$1\"\n").unwrap();
        assert!(run(
            &guard,
            &work,
            "sh \"$3\" \"$1\" \"$2\"",
            &[&sibling, &child_script, &grandchild_script]
        ));
        assert!(!run(
            &guard,
            &work,
            "sh \"$3\" \"$1\" \"$2\"",
            &[&file, &child_script, &grandchild_script]
        ));
        assert_eq!(std::fs::read(&file).unwrap(), b"original");
        #[cfg(target_os = "linux")]
        live_linux_escapes(&guard, &root, &work, &file);
    }

    /// Linux-only part of [`live_native_guard`]: metadata denials, nested
    /// user/mount namespace and nested bubblewrap escapes, `/proc` root, cwd
    /// and fd links of an unconfined same-UID process, and unchanged exit
    /// status and environment. Negative controls prove each denial is caused
    /// by the guard: the same `/proc` links are writable unguarded, and the
    /// same bubblewrap launch without ancestor binds can rename an ancestor.
    #[cfg(target_os = "linux")]
    fn live_linux_escapes(guard: &SharedAssetGuard, root: &Path, work: &Path, file: &Path) {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(file).unwrap().permissions().mode();
        assert!(!run(guard, work, "truncate -s 0 \"$1\"", &[file]));
        assert!(!run(guard, work, "touch -d 2000-01-01 \"$1\"", &[file]));
        let planted = root.join("planted");
        assert!(!run(guard, work, "ln -s / \"$1\"", &[&planted]));
        // Each nested shell receives the outer `$1 $2` explicitly. Positive
        // controls first prove nested user/mount namespaces and nested
        // bubblewrap work inside the guard, so the denials below come from the
        // locked read-only mounts, not from a refused namespace.
        let nested = work.join("nested");
        for script in [
            "unshare -Urm sh -c 'mount --bind \"$1\" \"$1\" && printf ok > \"$1/n1\"' sh \"$1\"",
            "bwrap --dev-bind / / -- sh -c 'printf ok > \"$1/n2\"' sh \"$1\"",
        ] {
            std::fs::create_dir_all(&nested).unwrap();
            assert!(run(guard, work, script, &[&nested]), "{script}");
        }
        assert_eq!(std::fs::read(nested.join("n1")).unwrap(), b"ok");
        assert_eq!(std::fs::read(nested.join("n2")).unwrap(), b"ok");
        for script in [
            "unshare -Urm sh -c 'umount -l \"$1\"; printf bad > \"$2\"' sh \"$1\" \"$2\"",
            "unshare -Urm sh -c 'mount -o remount,bind,rw \"$1\"; printf bad > \"$2\"' sh \"$1\" \"$2\"",
            "unshare -Urm sh -c 'chmod 600 \"$2\"' sh \"$1\" \"$2\"",
            "bwrap --dev-bind / / -- sh -c 'printf bad > \"$2\"' sh \"$1\" \"$2\"",
            "bwrap --dev-bind / / --bind \"$1\" \"$1\" -- sh -c 'printf bad > \"$2\"' sh \"$1\" \"$2\"",
        ] {
            assert!(!run(guard, work, script, &[root, file]), "{script}");
        }
        let base = root.parent().unwrap();
        let view = work.join("view");
        std::fs::create_dir(&view).unwrap();
        assert!(!run(
            guard,
            work,
            "unshare -Urm sh -c 'mount --rbind \"$1\" \"$2\" && mv \"$2/shared\" \"$2/moved\"' \
             sh \"$1\" \"$2\"",
            &[base, &view]
        ));
        assert!(!run(
            guard,
            work,
            "unshare -Urm sh -c 'mount --bind \"$1\" \"$2\"' sh \"$1\" \"$2\"",
            &[base, &view]
        ));
        let control = root.join("control");
        std::fs::write(&control, b"c").unwrap();
        let mut outside = std::process::Command::new("/bin/sh")
            .args(["-c", "exec 3>>\"$1\"; exec sleep 30", "--"])
            .arg(&control)
            .current_dir(root)
            .spawn()
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        let proc = PathBuf::from(format!("/proc/{}", outside.id()));
        let links = [
            proc.join("root").join(control.strip_prefix("/").unwrap()),
            proc.join("cwd/control"),
            proc.join("fd/3"),
        ];
        let unguarded = links.iter().all(|link| {
            std::fs::OpenOptions::new()
                .append(true)
                .open(link)
                .and_then(|mut handle| std::io::Write::write_all(&mut handle, b"u"))
                .is_ok()
        });
        let guarded = links
            .iter()
            .map(|link| run(guard, work, "printf bad >> \"$1\"", &[link]))
            .collect::<Vec<_>>();
        outside.kill().unwrap();
        outside.wait().unwrap();
        assert!(unguarded, "control: /proc links must be writable unguarded");
        assert_eq!(guarded, [false, false, false]);
        assert_eq!(std::fs::read(&control).unwrap(), b"cuuu");
        assert_eq!(std::fs::read(file).unwrap(), b"original");
        assert_eq!(std::fs::metadata(file).unwrap().permissions().mode(), mode);
        assert!(!planted.exists() && root.is_dir());

        let argv = guard
            .wrap(
                Path::new("/bin/sh"),
                &["-c".into(), "[ \"$GUARD_PROBE\" = kept ] && exit 7".into()],
            )
            .unwrap();
        let status = std::process::Command::new(&argv[0])
            .args(&argv[1..])
            .env("GUARD_PROBE", "kept")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(7));

        let moved = base.with_extension("control");
        let script = ["-c", "mv \"$1\" \"$2\"", "--"].map(String::from);
        let full = guard.bubblewrap_argv(Path::new("/bin/sh"), &script);
        let mut unbound = Vec::new();
        let mut index = 0;
        while index < full.len() {
            if full[index] == "--bind" {
                index += 3;
            } else {
                unbound.push(full[index].clone());
                index += 1;
            }
        }
        let renamed = std::process::Command::new(&unbound[0])
            .args(&unbound[1..])
            .arg(base)
            .arg(&moved)
            .status()
            .unwrap()
            .success();
        if renamed {
            std::fs::rename(&moved, base).unwrap();
        }
        assert!(renamed, "control: an unbound ancestor must be renamable");
        assert!(!run(guard, work, "mv \"$1\" \"$2\"", &[base, &moved]));
        assert!(root.is_dir() && !moved.exists());
    }

    /// Exercises Codex's native read carveout with managed configuration and
    /// checks that a metadata-only Claude process starts under this guard.
    /// All filesystem mutations are confined to a temporary directory below
    /// the real home; neither executable starts a model turn.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "requires native macOS plus CODEX_BIN, CLAUDE_BIN and NATIVE_GUARD_HOME"]
    fn live_native_codex_permissions_and_claude_startup() {
        let codex = PathBuf::from(std::env::var_os("CODEX_BIN").expect("CODEX_BIN"));
        let claude = PathBuf::from(std::env::var_os("CLAUDE_BIN").expect("CLAUDE_BIN"));
        assert!(codex.is_absolute() && claude.is_absolute());
        let home = PathBuf::from(std::env::var_os("NATIVE_GUARD_HOME").expect("NATIVE_GUARD_HOME"))
            .canonicalize()
            .unwrap();
        let temp = tempfile::Builder::new()
            .prefix("agent-run-native-guard-")
            .tempdir_in(&home)
            .unwrap();
        let base = temp.path().canonicalize().unwrap();
        let fixture = base.join("fixture");
        let root = fixture.join("shared");
        let work = base.join("work");
        let codex_home = base.join("codex-home");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir(&work).unwrap();
        std::fs::create_dir(&codex_home).unwrap();
        let file = root.join("asset");
        std::fs::write(&file, b"original").unwrap();
        let link = work.join("asset-link");
        std::os::unix::fs::symlink(&file, &link).unwrap();

        let config = format!(
            "[permissions.shared_probe]\nextends = \":workspace\"\n\
             [permissions.shared_probe.workspace_roots]\n{} = true\n\
             [permissions.shared_probe.filesystem]\n{} = \"read\"\n",
            serde_json::to_string(&base.to_str().unwrap()).unwrap(),
            serde_json::to_string(&root.to_str().unwrap()).unwrap()
        );
        std::fs::write(codex_home.join("config.toml"), config).unwrap();
        let path = format!("{}:/usr/bin:/bin", codex.parent().unwrap().display());

        // The closure runs only finite shell scripts; output carries the
        // diagnostic when a positive control fails before any denial counts.
        let run = |script: &str, args: &[&Path]| {
            let mut command = std::process::Command::new(&codex);
            command
                .arg("sandbox")
                .arg("--include-managed-config")
                .arg("-P")
                .arg("shared_probe")
                .arg("-C")
                .arg(&work)
                .arg("--")
                .arg("/bin/sh")
                .arg("-c")
                .arg(script)
                .arg("--")
                .args(args)
                .env_clear()
                .env("PATH", &path)
                .env("HOME", &home)
                .env("CODEX_HOME", &codex_home);
            command.output().unwrap()
        };

        let sibling = work.join("sibling");
        let readback = work.join("readback");
        let positive = run(
            "cat \"$1\" > \"$2\" && printf ok > \"$3\"",
            &[&link, &readback, &sibling],
        );
        assert!(
            positive.status.success(),
            "{}",
            String::from_utf8_lossy(&positive.stderr)
        );
        assert_eq!(std::fs::read(&readback).unwrap(), b"original");
        assert_eq!(std::fs::read(&sibling).unwrap(), b"ok");
        let renamed = work.join("renamed");
        let positive_rename = run(
            "mv \"$1\" \"$2\" && mv \"$2\" \"$1\"",
            &[&sibling, &renamed],
        );
        assert!(
            positive_rename.status.success(),
            "{}",
            String::from_utf8_lossy(&positive_rename.stderr)
        );
        assert!(!run("printf bad > \"$1\"", &[&file]).status.success());
        assert!(!run("printf bad > \"$1\"", &[&link]).status.success());
        assert!(!run("chmod 600 \"$1\"", &[&file]).status.success());
        assert!(!run("rm \"$1\"", &[&file]).status.success());
        let escaped = work.join("escaped");
        assert!(!run("ln \"$1\" \"$2\"", &[&file, &escaped]).status.success());
        assert!(!run("mv \"$1\" \"$2\"", &[&file, &escaped]).status.success());
        assert!(!run("mv \"$1\" \"$2\"", &[&root, &escaped]).status.success());
        let moved_fixture = fixture.with_extension("moved");
        assert!(!run("mv \"$1\" \"$2\"", &[&fixture, &moved_fixture])
            .status
            .success());
        assert_eq!(std::fs::read(&file).unwrap(), b"original");
        assert!(!escaped.exists());

        let guard = SharedAssetGuard::new(&root).unwrap();
        let argv = guard.wrap(&claude, &["--version".to_string()]).unwrap();
        let version = std::process::Command::new(&argv[0])
            .args(&argv[1..])
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &base)
            .current_dir(&work)
            .output()
            .unwrap();
        assert!(
            version.status.success(),
            "{}",
            String::from_utf8_lossy(&version.stderr)
        );
        assert!(!version.stdout.is_empty());
    }
}
