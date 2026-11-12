//! Test steps of `cargo xtask check` under the Rust gate standard.
//!
//! Tests run under cargo-nextest (one process per test, every failure
//! reported), doctests and `harness = false` targets under `cargo test`, and
//! test temporaries go to a private tmpfs directory when one is usable.

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

/// `harness = false` integration targets that nextest cannot list; they run
/// with `cargo test --test` instead (see `.config/nextest.toml`).
const CUSTOM_HARNESS: &[(&str, &str)] = &[("agent-run-platform", "launch_fd6")];

/// In-process test locks and the one-thread nextest group listing their users.
/// nextest runs each test in its own process, so the lock alone no longer
/// serializes them.
const LOCK_GROUPS: &[(&str, &str, &str)] = &[(
    "crates/agent-run/tests/cli_seam.rs",
    "FOLLOW_TESTS",
    "follow",
)];

/// Runs the structural test checks, then every test configuration of the gate.
/// Every test command runs even after an earlier one failed, so one gate run
/// reports all failing configurations.
///
/// # Errors
/// A structural violation, or the list of failed test commands.
pub fn run_tests(root: &Path) -> Result<(), String> {
    integration_tests_included(root)?;
    lock_users_grouped(root)?;
    let scratch = TestScratch::create();
    let threads = test_threads();
    let mut failed = Vec::new();
    for features in [Some("--all-features"), None] {
        let mut nextest = vec![
            "nextest",
            "run",
            "--offline",
            "--locked",
            "--workspace",
            "--no-fail-fast",
        ];
        nextest.extend(features);
        if let Some(threads) = threads.as_deref() {
            nextest.extend(["--test-threads", threads]);
        }
        failed.extend(scratch.cargo(root, &nextest).err());
        for (package, target) in CUSTOM_HARNESS {
            let mut custom = vec![
                "test",
                "--offline",
                "--locked",
                "-p",
                package,
                "--test",
                target,
            ];
            custom.extend(features);
            failed.extend(scratch.cargo(root, &custom).err());
        }
        let mut doctests = vec![
            "test",
            "--offline",
            "--locked",
            "--workspace",
            "--doc",
            "--no-fail-fast",
        ];
        doctests.extend(features);
        failed.extend(scratch.cargo(root, &doctests).err());
    }
    if failed.is_empty() {
        Ok(())
    } else {
        Err(failed.join("; "))
    }
}

/// The operator's `RUST_TEST_THREADS` (for example `1` in release acceptance),
/// which nextest ignores unless it is passed as `--test-threads`. An explicit
/// `NEXTEST_TEST_THREADS` wins.
fn test_threads() -> Option<String> {
    if std::env::var_os("NEXTEST_TEST_THREADS").is_some() {
        return None;
    }
    std::env::var("RUST_TEST_THREADS")
        .ok()
        .filter(|value| value.parse::<u16>().is_ok_and(|threads| threads > 0))
}

/// With `autotests = false` a `tests/*.rs` file that is neither a module of its
/// crate's `tests/it/main.rs` nor a `harness = false` target would silently
/// never build; reject that, a commented-out module line, and a package with an
/// `it` binary whose manifest lets Cargo build each file separately again.
fn integration_tests_included(root: &Path) -> Result<(), String> {
    let crates = fs::read_dir(root.join("crates")).map_err(|error| format!("crates: {error}"))?;
    let packages = crates
        .flatten()
        .map(|entry| entry.path())
        .chain([root.join("xtask")]);
    for package in packages {
        let tests = package.join("tests");
        let Ok(index) = fs::read_to_string(tests.join("it/main.rs")) else {
            continue;
        };
        let manifest = fs::read_to_string(package.join("Cargo.toml")).unwrap_or_default();
        if !manifest
            .lines()
            .any(|line| line.trim() == "autotests = false")
        {
            return Err(format!(
                "{} must set `autotests = false` for its single `it` test binary",
                package.join("Cargo.toml").display()
            ));
        }
        let modules: Vec<&str> = index
            .lines()
            .map(str::trim)
            .filter(|line| line.starts_with("#[path = \"../"))
            .collect();
        let files =
            fs::read_dir(&tests).map_err(|error| format!("{}: {error}", tests.display()))?;
        for file in files.flatten() {
            let path = file.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if path.extension().is_some_and(|extension| extension == "rs")
                && !modules.contains(&format!("#[path = \"../{name}\"]").as_str())
                && !manifest.contains(&format!("path = \"tests/{name}\""))
            {
                return Err(format!(
                    "{} is not a module of tests/it/main.rs",
                    path.display()
                ));
            }
        }
    }
    Ok(())
}

/// Every test taking a lock from [`LOCK_GROUPS`] must be listed in the filter of
/// the `.config/nextest.toml` override that assigns its one-thread group.
fn lock_users_grouped(root: &Path) -> Result<(), String> {
    let config = fs::read_to_string(root.join(".config/nextest.toml"))
        .map_err(|error| format!(".config/nextest.toml: {error}"))?;
    for (file, lock, group) in LOCK_GROUPS {
        let assignment = format!("test-group = \"{group}\"");
        let end = config
            .find(&assignment)
            .ok_or_else(|| format!("no override assigns nextest group {group}"))?;
        let start = config[..end]
            .rfind("[[profile.default.overrides]]")
            .ok_or_else(|| format!("nextest group {group} is assigned outside an override"))?;
        let filter = &config[start..end];
        let source =
            fs::read_to_string(root.join(file)).map_err(|error| format!("{file}: {error}"))?;
        for (at, _) in source.match_indices(&format!("{lock}.")) {
            let name = source[..at]
                .rfind("fn ")
                .and_then(|start| source[start + 3..].split(['(', '<']).next())
                .ok_or_else(|| format!("{lock} used outside a function in {file}"))?;
            if !filter.contains(&format!("::{name})")) {
                return Err(format!(
                    "{name} takes {lock} but is not in nextest group {group}"
                ));
            }
        }
    }
    Ok(())
}

/// Private directory for test temporaries on tmpfs (`/dev/shm`), so
/// filesystem-heavy tests avoid slow disk sync. Tests keep the inherited `TMPDIR`
/// when tmpfs is absent, cannot execute files, has less than 1 GiB free, or
/// `XTASK_TEST_TMPFS=0` is set. The directory is removed on drop; directories
/// left by exited runs of the same PID namespace are removed on the next start.
struct TestScratch(Option<PathBuf>);

impl TestScratch {
    /// Creates a fresh `0700` directory, or none when tmpfs is unsuitable.
    fn create() -> Self {
        if std::env::var_os("XTASK_TEST_TMPFS").is_some_and(|value| value == "0") {
            return Self(None);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            let base = Path::new("/dev/shm");
            if base.is_dir() {
                let namespace = pid_namespace();
                remove_stale_scratch(base, &namespace);
                let dir = base.join(format!(
                    "xtask-{}-{namespace}-{}",
                    std::process::id(),
                    random_suffix()
                ));
                if fs::DirBuilder::new().mode(0o700).create(&dir).is_ok() {
                    if scratch_usable(&dir) {
                        return Self(Some(dir));
                    }
                    drop(fs::remove_dir_all(&dir));
                }
            }
        }
        Self(None)
    }

    /// Runs `cargo <args>` with `TMPDIR`, `TMP` and `TEMP` set to the directory.
    fn cargo(&self, root: &Path, args: &[&str]) -> Result<(), String> {
        eprintln!("xtask: cargo {}", args.join(" "));
        let mut command = Command::new("cargo");
        command.current_dir(root).args(args);
        if let Some(dir) = &self.0 {
            for name in ["TMPDIR", "TMP", "TEMP"] {
                command.env(name, dir);
            }
        }
        let status = command
            .status()
            .map_err(|error| format!("cargo: {error}"))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("cargo {} failed", args.join(" ")))
        }
    }
}

impl Drop for TestScratch {
    fn drop(&mut self) {
        if let Some(dir) = &self.0 {
            owner_writable(dir);
            // Best effort: a leftover is removed by the next run's stale cleanup.
            drop(fs::remove_dir_all(dir));
        }
    }
}

/// True when a script in `dir` can execute and at least 1 GiB is free there.
#[cfg(unix)]
fn scratch_usable(dir: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    let probe = dir.join("probe");
    let executes = fs::write(&probe, "#!/bin/sh\nexit 0\n").is_ok()
        && fs::set_permissions(&probe, fs::Permissions::from_mode(0o700)).is_ok()
        && Command::new(&probe)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success());
    drop(fs::remove_file(&probe));
    // POSIX `df -Pk`: the second line's fourth column is the free space in KiB.
    let free_kib = Command::new("df")
        .arg("-Pk")
        .arg(dir)
        .output()
        .ok()
        .and_then(|output| {
            String::from_utf8(output.stdout)
                .ok()?
                .lines()
                .nth(1)?
                .split_whitespace()
                .nth(3)?
                .parse::<u64>()
                .ok()
        });
    executes && free_kib.is_some_and(|kib| kib >= 1024 * 1024)
}

/// The inode of this process's PID namespace (`/proc/self/ns/pid`), or `0`
/// when unavailable, so stale cleanup never judges another namespace's PIDs.
#[cfg(unix)]
fn pid_namespace() -> String {
    fs::read_link("/proc/self/ns/pid")
        .ok()
        .and_then(|link| {
            link.to_str()?
                .strip_prefix("pid:[")?
                .strip_suffix(']')
                .map(str::to_owned)
        })
        .filter(|inode| !inode.is_empty() && inode.bytes().all(|byte| byte.is_ascii_digit()))
        .unwrap_or_else(|| "0".to_owned())
}

/// Sixteen hex digits from `/dev/urandom`, falling back to the clock.
#[cfg(unix)]
fn random_suffix() -> String {
    use std::io::Read as _;
    let mut bytes = [0_u8; 8];
    let random = fs::File::open("/dev/urandom").and_then(|mut file| file.read_exact(&mut bytes));
    if random.is_err() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        bytes = (nanos as u64).to_le_bytes();
    }
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Removes this user's scratch directories from the same PID namespace whose
/// creating process has exited.
#[cfg(unix)]
fn remove_stale_scratch(base: &Path, namespace: &str) {
    use std::os::unix::fs::MetadataExt;
    let Ok(me) = fs::metadata("/proc/self") else {
        return;
    };
    let Ok(entries) = fs::read_dir(base) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let mut parts = name
            .to_str()
            .and_then(|name| name.strip_prefix("xtask-"))
            .unwrap_or_default()
            .split('-');
        let (Some(pid), Some(owner_namespace)) = (parts.next(), parts.next()) else {
            continue;
        };
        // Only a canonical decimal PID names a process; `0` means the namespace
        // is unknown, so nothing can be judged stale.
        if pid.parse::<u32>().map(|value| value.to_string()).as_deref() != Ok(pid)
            || namespace == "0"
            || owner_namespace != namespace
        {
            continue;
        }
        let owned = entry
            .metadata()
            .is_ok_and(|meta| meta.is_dir() && meta.uid() == me.uid());
        if owned && !Path::new("/proc").join(pid).exists() {
            owner_writable(&entry.path());
            drop(fs::remove_dir_all(entry.path()));
        }
    }
}

/// Restores owner access on directories so read-only trees left by tests do
/// not block removal.
fn owner_writable(dir: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        drop(fs::set_permissions(dir, fs::Permissions::from_mode(0o700)));
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                    owner_writable(&entry.path());
                }
            }
        }
    }
}
