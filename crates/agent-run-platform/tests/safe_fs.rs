//! Descriptor-anchored, no-follow primitives (`src/fs.rs`) and agent-path
//! construction (`src/paths.rs`). Each test names the Python behavior it
//! mirrors so a regression here also flags the Python test to re-check.
use agent_run_platform::{fs, paths};
use std::{os::unix::fs::PermissionsExt, path::Path};

fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("agent-run-safe-fs-")
        .tempdir_in(std::env::var_os("AGENT_RUN_TEST_TMP").unwrap_or_else(|| "/tmp".into()))
        .expect("tempdir")
}

/// Mirrors `tests/test_verify.py`'s no-follow-open cases: a symlink swapped
/// in for a managed file must never be read through.
#[test]
fn read_refuses_a_symlink_swapped_in_for_the_target_file() {
    let root = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    dir.write(Path::new("secret.txt"), b"real", 0o600).unwrap();

    let outside = tempdir();
    std::fs::write(outside.path().join("other"), b"OUTSIDE").unwrap();
    std::fs::remove_file(root.path().join("secret.txt")).unwrap();
    std::os::unix::fs::symlink(outside.path().join("other"), root.path().join("secret.txt"))
        .unwrap();

    let err = dir.read(Path::new("secret.txt"), 1024).unwrap_err();
    assert!(!format!("{err}").is_empty());
    assert!(dir.optional(Path::new("secret.txt"), 1024).is_err());
}

/// Mirrors `_open_managed_parent`'s per-component `O_NOFOLLOW` in
/// `adapters/home.py`: a symlink swapped in for an intermediate directory
/// must not be traversed into.
#[test]
fn write_refuses_a_symlink_swapped_in_for_a_parent_directory() {
    let root = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    dir.directory(Path::new("a")).unwrap();
    dir.write(Path::new("a/file.txt"), b"first", 0o600).unwrap();

    let outside = tempdir();
    std::fs::remove_file(root.path().join("a/file.txt")).unwrap();
    std::fs::remove_dir(root.path().join("a")).unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("a")).unwrap();

    let result = dir.write(Path::new("a/file.txt"), b"second", 0o600);
    assert!(
        result.is_err(),
        "must refuse to write through a symlinked parent"
    );
    assert!(
        !outside.path().join("file.txt").exists(),
        "the symlink target must not receive the write"
    );
}

/// Mirrors `tests/test_paths.py`'s absolute/`..` rejection for owned paths.
#[test]
fn relative_paths_reject_absolute_and_traversal() {
    assert!(fs::relative(Path::new("/etc/passwd")).is_err());
    assert!(fs::relative(Path::new("../escape")).is_err());
    assert!(fs::relative(Path::new("a/../../escape")).is_err());
    assert!(fs::relative(Path::new(".")).is_err());
    assert!(fs::relative(Path::new("")).is_err());
    assert!(fs::relative(Path::new("a/b")).is_ok());
}

#[test]
fn dir_write_and_directory_reject_absolute_and_traversing_paths() {
    let root = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    assert!(dir.write(Path::new("/abs"), b"x", 0o600).is_err());
    assert!(dir.write(Path::new("../abs"), b"x", 0o600).is_err());
    assert!(dir.directory(Path::new("../abs")).is_err());
}

/// Mirrors `read_answer_payload`'s independent size bound in `verify.py`.
#[test]
fn read_enforces_the_caller_supplied_byte_bound() {
    let root = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    dir.write(Path::new("f"), b"0123456789", 0o600).unwrap();
    assert!(dir.read(Path::new("f"), 9).is_err());
    assert_eq!(dir.read(Path::new("f"), 10).unwrap(), b"0123456789");
    assert_eq!(dir.read(Path::new("f"), 4096).unwrap(), b"0123456789");
}

#[test]
fn optional_read_distinguishes_missing_from_error() {
    let root = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    assert_eq!(dir.optional(Path::new("absent"), 1024).unwrap(), None);
    dir.write(Path::new("present"), b"hi", 0o600).unwrap();
    assert_eq!(
        dir.optional(Path::new("present"), 1024).unwrap(),
        Some(b"hi".to_vec())
    );
}

/// Mirrors `write_managed_file`'s private (`0600`) file mode.
#[test]
fn write_applies_the_requested_mode_exactly() {
    let root = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    dir.write(Path::new("data"), b"x", 0o600).unwrap();
    dir.write(Path::new("run.sh"), b"x", 0o700).unwrap();
    let data_mode = std::fs::metadata(root.path().join("data"))
        .unwrap()
        .permissions()
        .mode();
    let exec_mode = std::fs::metadata(root.path().join("run.sh"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(data_mode & 0o777, 0o600);
    assert_eq!(exec_mode & 0o777, 0o700);
}

/// A non-regular target (a FIFO, say) must never be reported as a readable
/// artifact, matching `_open_regular_descriptor`'s type check (verify.py).
#[test]
fn open_file_refuses_a_special_file() {
    let root = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    let fifo = root.path().join("pipe");
    let c_path = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
    // SAFETY: mkfifo only creates the named special file; the path is valid
    // UTF-8 owned by this test's temp directory.
    let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
    assert_eq!(rc, 0, "mkfifo must succeed for this test to be meaningful");
    assert!(dir.read(Path::new("pipe"), 1024).is_err());
}

#[test]
fn remove_deletes_through_the_owned_descriptor() {
    let root = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    dir.write(Path::new("gone"), b"x", 0o600).unwrap();
    dir.remove(Path::new("gone")).unwrap();
    assert_eq!(dir.optional(Path::new("gone"), 1024).unwrap(), None);
    assert!(dir.remove(Path::new("../escape")).is_err());
}

/// Mirrors `tests/test_paths.py::test_agent_dir_rejects_escape`-style checks:
/// `paths.py`'s `_require_beneath` guard on a freeform component.
#[test]
fn runtime_skills_dir_rejects_traversal() {
    let root = tempdir();
    let home = Some(root.path().to_path_buf());
    assert!(paths::runtime_skills_dir("codex", home.clone()).is_ok());
    assert!(paths::runtime_skills_dir("../../etc", home.clone()).is_err());
    assert!(paths::runtime_skills_dir("", home.clone()).is_err());
    assert!(paths::runtime_skills_dir("   ", home).is_err());
}

/// Mirrors `paths.py:agent_dir`'s `validate_agent_id` call: a well-formed id
/// is accepted, and traversal or freeform text is rejected outright, not
/// silently joined beneath `agents/`.
#[test]
fn agent_dir_requires_a_well_formed_agent_id() {
    let root = tempdir();
    let home = Some(root.path().to_path_buf());
    assert!(paths::agent_dir("ag-20260101-000000-abcdef0123", home.clone()).is_ok());
    assert!(paths::agent_dir("../../etc/passwd", home.clone()).is_err());
    assert!(paths::agent_dir("not-an-agent-id", home).is_err());
}

/// Mirrors `paths.py:create_agent_dir`'s private (`0700`) directory mode on
/// every level it creates.
#[test]
fn create_agent_dir_is_private_and_beneath_agents_root() {
    let root = tempdir();
    // `agent_run_home` canonicalizes an already-existing root (e.g. macOS
    // resolves `/tmp` to `/private/tmp`), so compare against the same
    // canonical form rather than the tempdir's own un-resolved path.
    let root_canon = root.path().canonicalize().unwrap();
    let home = Some(root.path().to_path_buf());
    let id = "ag-20260101-000000-abcdef0123";
    let created = paths::create_agent_dir(id, home.clone()).unwrap();
    assert_eq!(created, paths::agent_dir(id, home).unwrap());
    assert!(created.starts_with(root_canon.join("agents")));
    let mode = std::fs::metadata(&created).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o700);
    let agents_mode = std::fs::metadata(root.path().join("agents"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(agents_mode & 0o777, 0o700);
}

#[test]
fn config_and_state_db_paths_sit_directly_under_home() {
    let root = tempdir();
    let root_canon = root.path().canonicalize().unwrap();
    let home = Some(root.path().to_path_buf());
    assert_eq!(
        paths::config_path(home.clone()).unwrap(),
        root_canon.join("config.toml")
    );
    assert_eq!(
        paths::state_db_path(home).unwrap(),
        root_canon.join("state.db")
    );
}
