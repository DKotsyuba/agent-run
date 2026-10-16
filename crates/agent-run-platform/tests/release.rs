//! Local sealed-release inventory: `verify` accepts only a closed set of
//! regular files that the `SHA256SUMS` manifest lists, while supported legacy
//! (no-TUI) and complete native layouts stay valid. This is local candidate
//! integrity only, not a remote archive attestation.

use agent_run_platform::{fs::sha256, release::verify};
use std::{
    fs,
    os::unix::{fs::symlink, net::UnixListener},
    path::Path,
};

/// Files of a supported legacy release: no deployment helper, no observer.
const LEGACY: &[&str] = &["bin/agent-run", "collectors/codex.sh", "metadata.json"];
/// Files of a complete native release including the bundled observer.
const NATIVE: &[&str] = &[
    "bin/agent-run",
    "bin/agent-run-deploy",
    "bin/agent-run-tui",
    "collectors/codex.sh",
    "services/probe.cjs",
    "metadata.json",
];

/// Seals `files` below `root` the way `xtask` does: manifest of every file,
/// then `SHA256SUMS`, then the `COMPLETE` marker.
fn seal(root: &Path, files: &[&str]) {
    let mut manifest = String::new();
    for name in files {
        let path = root.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let bytes = if *name == "metadata.json" {
            b"{\"version\":\"1.0.0\",\"format\":1,\"schema_version\":32}\n".to_vec()
        } else {
            format!("content of {name}").into_bytes()
        };
        fs::write(&path, &bytes).unwrap();
        manifest.push_str(&format!("{}  {name}\n", sha256(&bytes)));
    }
    fs::write(root.join("SHA256SUMS"), manifest).unwrap();
    fs::write(root.join("COMPLETE"), "complete\n").unwrap();
}

/// Seals a fixture and returns its disposable owner.
fn sealed(files: &[&str]) -> tempfile::TempDir {
    let temporary = tempfile::tempdir().unwrap();
    seal(temporary.path(), files);
    temporary
}

/// Supported legacy and complete native inventories verify unchanged.
#[test]
fn complete_and_legacy_inventories_verify() {
    verify(sealed(LEGACY).path()).unwrap();
    verify(sealed(NATIVE).path()).unwrap();
}

/// An executable added after sealing, with or without a TUI listing, is refused.
#[test]
fn unlisted_executable_is_refused() {
    let legacy = sealed(LEGACY);
    fs::write(legacy.path().join("bin/agent-run-tui"), "added observer").unwrap();
    assert_eq!(
        verify(legacy.path()).unwrap_err(),
        "release contains an unlisted file"
    );
    let native = sealed(NATIVE);
    fs::write(native.path().join("bin/agent-run-extra"), "added").unwrap();
    assert_eq!(
        verify(native.path()).unwrap_err(),
        "release contains an unlisted file"
    );
}

/// Unlisted assets are refused at top level and in nested or new directories.
#[test]
fn unlisted_assets_are_refused_anywhere() {
    for added in ["extra.txt", "collectors/extra.sh", "services/deep/er/x.cjs"] {
        let release = sealed(NATIVE);
        let path = release.path().join(added);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, "added").unwrap();
        assert!(verify(release.path()).is_err(), "{added}");
    }
}

/// Symlinks and special files are refused whether listed, unlisted or unreachable.
#[test]
fn links_and_special_files_are_refused() {
    let unlisted = sealed(LEGACY);
    symlink("bin/agent-run", unlisted.path().join("bin/alias")).unwrap();
    assert!(verify(unlisted.path()).is_err());

    let socket = sealed(LEGACY);
    let _listener = UnixListener::bind(socket.path().join("bin/agent-run-tui")).unwrap();
    assert!(verify(socket.path()).is_err());

    // A listed entry replaced by a link to identical bytes must not pass.
    let listed = sealed(LEGACY);
    let target = listed.path().join("bin/agent-run");
    let bytes = fs::read(&target).unwrap();
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("bytes"), bytes).unwrap();
    fs::remove_file(&target).unwrap();
    symlink(outside.path().join("bytes"), &target).unwrap();
    assert!(verify(listed.path()).is_err());

    // A dangling link is refused as a link, not reported as a missing file.
    let dangling = sealed(LEGACY);
    symlink("missing", dangling.path().join("collectors/dangling")).unwrap();
    assert!(verify(dangling.path()).is_err());
}

/// The manifest cannot list seal files, non-normal paths or the same file twice.
#[test]
fn manifest_paths_must_be_normal_and_unique() {
    for line in [
        format!("{}  COMPLETE\n", sha256(b"complete\n")),
        format!("{}  ./bin/agent-run\n", sha256(b"content of bin/agent-run")),
        format!("{}  bin//agent-run\n", sha256(b"content of bin/agent-run")),
        format!("{}  bin/agent-run\n", sha256(b"content of bin/agent-run")),
    ] {
        let release = sealed(LEGACY);
        let mut manifest = fs::read_to_string(release.path().join("SHA256SUMS")).unwrap();
        manifest.push_str(&line);
        fs::write(release.path().join("SHA256SUMS"), manifest).unwrap();
        assert!(verify(release.path()).is_err(), "{line}");
    }
}

/// Existing guarantees remain: tampered bytes, missing entry points, missing seal.
#[test]
fn existing_seal_checks_still_hold() {
    let tampered = sealed(LEGACY);
    fs::write(tampered.path().join("bin/agent-run"), "changed").unwrap();
    assert!(verify(tampered.path()).unwrap_err().contains("corrupt"));

    let incomplete = sealed(LEGACY);
    fs::remove_file(incomplete.path().join("COMPLETE")).unwrap();
    assert!(verify(incomplete.path()).is_err());

    let missing_entry = sealed(&["bin/agent-run", "collectors/codex.sh"]);
    assert!(verify(missing_entry.path()).is_err());
}

/// Child-process entry for the bounded FIFO checks: when the parent sets
/// `AGENT_RUN_FIFO_RELEASE` it verifies that release and prints the outcome;
/// otherwise this is a no-op, so a normal run does nothing here.
#[test]
fn fifo_child_entry() {
    let Some(release) = std::env::var_os("AGENT_RUN_FIFO_RELEASE") else {
        return;
    };
    println!("OUTCOME:{:?}", verify(Path::new(&release)));
}

/// Verifies `release` in a child copy of this test executable and returns its
/// printed outcome, or `None` when it has not exited within five seconds. A
/// child that blocks on a FIFO is killed and reaped here, so a broken verifier
/// can strand no thread or process.
fn bounded_verify(release: &Path) -> Option<String> {
    use std::io::Read;
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "fifo_child_entry", "--nocapture"])
        .env("AGENT_RUN_FIFO_RELEASE", release)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() {
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            return None;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let mut output = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut output)
        .unwrap();
    output
        .lines()
        .find_map(|line| line.strip_prefix("OUTCOME:").map(str::to_owned))
}

/// Replaces `name` below `root` with a named pipe (no reader or writer).
fn replace_with_fifo(root: &Path, name: &str) -> std::path::PathBuf {
    let path = root.join(name);
    fs::remove_file(&path).unwrap();
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap()
            .success()
    );
    path
}

/// Asserts a FIFO standing in for `seal_file` is refused promptly as a special
/// file; the verifier must classify entries before it opens either seal file.
fn assert_fifo_seal_refused(seal_file: &str) {
    let release = sealed(LEGACY);
    replace_with_fifo(release.path(), seal_file);
    assert_eq!(
        bounded_verify(release.path()).as_deref(),
        Some(r#"Err("release contains a link or special file")"#),
        "{seal_file}"
    );
}

/// A FIFO `COMPLETE` is refused without blocking the verifier.
#[test]
fn fifo_complete_is_refused_without_blocking() {
    assert_fifo_seal_refused("COMPLETE");
}

/// A FIFO `SHA256SUMS` beside a valid `COMPLETE` is refused without blocking.
#[test]
fn fifo_manifest_is_refused_without_blocking() {
    assert_fifo_seal_refused("SHA256SUMS");
}
