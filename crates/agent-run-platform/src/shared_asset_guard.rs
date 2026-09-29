//! Read-only shared asset roots behind the native macOS launch sandbox.
//!
//! The target storage model keeps one central immutable payload store and
//! exposes it to each private home through symlinks. Same-UID file
//! permissions cannot enforce that model, because the launched child owns
//! the same UID and can rewrite, chmod, unlink or rename the shared bytes.
//! This module proves the smallest launch guard for qualified macOS hosts:
//! [`SharedAssetGuard::wrap`] rewrites a plain `program args…` argv into
//! `sandbox-exec -p <profile> -D ROOT=<canonical> … -- program args…`. The
//! profile denies every write-shaped operation (`file-write*`, which covers
//! open-for-write, create, unlink, rename, link, chmod and chown on this OS)
//! whose path is the canonical shared root or anything beneath it, and denies
//! those operations on each ancestor directory of the root as one exact
//! path, so a writable ancestor cannot be renamed to move the root out from
//! under the deny rule. Every filesystem path travels only in `-D` parameter
//! arguments — never embedded in the profile text — and ancestor parameters
//! are escaped as Seatbelt regex literals, so quoted or metacharacter paths
//! cannot widen or redirect the rules. Reads of shared assets through
//! private-home symlinks and ordinary writes inside the child's own workdir
//! stay allowed; the wrapper only adds denials and never loosens any other
//! constraint. The sandbox is inherited by every descendant of the wrapped
//! child. Before launch, a bounded no-follow inode scan refuses external
//! hardlink aliases while permitting links wholly inside the shared tree.
//!
//! The argv rewrite is meant to be applied to a future `LaunchPlan` in
//! `agent-run-adapters`; this module owns only the verified transformation.
//! It never claims protection over unrelated pre-existing processes (for
//! example MCP servers started elsewhere): only the wrapped child and its
//! descendants are constrained.
use std::{
    collections::HashMap,
    ffi::OsString,
    fmt, io,
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
};

/// Absolute path of the native Seatbelt wrapper used on qualified macOS hosts.
pub const MACOS_SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

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
    /// Given the intended `program` and its `args`, returns
    /// `sandbox-exec -p <profile> -D <parameters>… -- program args…` ready
    /// for the spawn call a `LaunchPlan` already drives. On macOS the
    /// wrapper first verifies `/usr/bin/sandbox-exec` is an executable file
    /// and that [`Self::verify_no_hardlink_aliases`] passes, so a launch
    /// cannot proceed while an alias could bypass the path-based deny rules.
    /// Non-macOS hosts return [`SharedAssetGuardError::UnsupportedPlatform`]
    /// and must not fall back to copying assets into private homes. No
    /// process is spawned and no environment or approval behavior changes.
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
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (program, args);
            Err(SharedAssetGuardError::UnsupportedPlatform(
                "Seatbelt launch profiles exist only on macOS; refusing to fake a guard",
            ))
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

#[cfg(test)]
/// Pure validation checks and a qualified-host Seatbelt behavior check.
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
    #[cfg(target_os = "macos")]
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
    /// Requires a qualified macOS host that permits applying Seatbelt profiles.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "requires native sandbox-exec application on a qualified macOS host"]
    fn live_native_guard() {
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
