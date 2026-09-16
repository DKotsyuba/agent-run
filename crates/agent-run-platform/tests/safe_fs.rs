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

/// Returns a fault hook that fails exactly at `target`, simulating a crash there.
fn fail_at(target: fs::FaultPoint) -> impl Fn(fs::FaultPoint) -> agent_run_domain::Result<()> {
    move |point| {
        if point == target {
            Err(agent_run_domain::error::invalid("simulated crash"))
        } else {
            Ok(())
        }
    }
}

/// Returns every `.tmp` entry left directly beneath `directory`.
fn temporaries(directory: &Path) -> Vec<String> {
    std::fs::read_dir(directory)
        .expect("readable directory")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name.ends_with(".tmp"))
        .collect()
}

/// Mirrors `tests/test_adapter_home.py::AdapterHomeTests::test_managed_files_are_private_atomic_and_content_hashed`
#[test]
fn managed_files_are_private_atomic_and_content_hashed() {
    let root = tempdir();
    let home = root.path().join("generated");
    fs::private_dir(&home).unwrap();
    let dir = fs::Dir::open(&home).unwrap();

    dir.write(Path::new("settings/config.toml"), b"first", 0o600)
        .unwrap();
    let target = home.join("settings/config.toml");
    // The exact digest Python records for the same payload.
    assert_eq!(
        fs::sha256(b"first"),
        "a7937b64b8caa58f03721bb6bacf5c78cb235febe0e70b1b84cd99541461a08e"
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"first");
    assert_eq!(
        std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
        0o600
    );

    dir.write(Path::new("settings/config.toml"), b"second", 0o600)
        .unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"second");
    assert!(
        temporaries(target.parent().unwrap()).is_empty(),
        "a completed publish leaves no temporary behind"
    );
}

/// Mirrors `tests/test_adapter_home.py::AdapterHomeTests::test_managed_paths_refuse_traversal_and_symlink_escape`
#[test]
fn managed_paths_refuse_traversal_and_symlink_escape() {
    let root = tempdir();
    let outside = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("linked")).unwrap();

    assert!(dir.write(Path::new("../outside"), b"no", 0o600).is_err());
    assert!(dir
        .write(Path::new("linked/outside"), b"no", 0o600)
        .is_err());
    assert!(
        !outside.path().join("outside").exists(),
        "a symlinked parent must never receive the payload"
    );
}

/// Mirrors `tests/test_adapter_home.py::AdapterHomeTests::test_new_parent_is_synced_before_file_publication`
#[test]
fn new_parent_is_created_before_file_publication() {
    let root = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    // A crash while the payload is still in its temporary file: the freshly
    // created parent is already on disk (and, in `Dir::parent`, already synced)
    // while the target name does not exist yet.
    let fault = fail_at(fs::FaultPoint::MidWrite);
    assert!(dir
        .write_seamed(Path::new("nested/answer.md"), b"done", 0o600, Some(&fault))
        .is_err());
    let parent = root.path().join("nested");
    assert!(parent.is_dir(), "the parent is created before publication");
    assert_eq!(
        std::fs::metadata(&parent).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert!(!parent.join("answer.md").exists());

    dir.write(Path::new("nested/answer.md"), b"done", 0o600)
        .unwrap();
    assert_eq!(std::fs::read(parent.join("answer.md")).unwrap(), b"done");
}

/// Mirrors `tests/test_adapter_home.py::AdapterHomeTests::test_managed_replace_fsyncs_file_then_parent_directory`
#[test]
fn replacement_is_published_only_after_its_own_bytes_are_durable() {
    let root = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    dir.write(Path::new("answer.md"), b"original", 0o600)
        .unwrap();

    // Before the rename the published name still holds the previous bytes.
    let fault = fail_at(fs::FaultPoint::BeforeRename);
    assert!(dir
        .write_seamed(Path::new("answer.md"), b"done", 0o600, Some(&fault))
        .is_err());
    assert_eq!(
        std::fs::read(root.path().join("answer.md")).unwrap(),
        b"original"
    );

    // After the rename, and before the parent directory is synced, the name
    // already holds the complete new payload -- never a partial one.
    let fault = fail_at(fs::FaultPoint::AfterRename);
    assert!(dir
        .write_seamed(Path::new("answer.md"), b"done", 0o600, Some(&fault))
        .is_err());
    assert_eq!(
        std::fs::read(root.path().join("answer.md")).unwrap(),
        b"done"
    );
}

/// Mirrors `tests/test_adapter_home.py::AdapterHomeTests::test_rejected_parent_and_temp_creation_close_every_descriptor`
#[test]
fn rejected_parent_and_temp_creation_close_every_descriptor() {
    let root = tempdir();
    let outside = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("linked")).unwrap();

    let open_descriptors = || {
        std::fs::read_dir("/dev/fd")
            .expect("descriptor list")
            .count()
    };
    // Descriptor counts are process-global and the sibling tests in this binary
    // open and close files on other threads, so this bounds growth across many
    // rejections instead of demanding exact equality: one descriptor leaked per
    // rejection would add 200, which no amount of sibling noise accounts for.
    let noise = 16;
    let baseline = open_descriptors();
    for _ in 0..200 {
        assert!(dir.write(Path::new("linked/file"), b"no", 0o600).is_err());
    }
    assert!(
        open_descriptors() <= baseline + noise,
        "a rejected parent must not retain a descriptor"
    );

    // A parent that cannot accept a new entry fails temporary creation itself.
    dir.directory(Path::new("locked")).unwrap();
    let locked = root.path().join("locked");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o500)).unwrap();
    let baseline = open_descriptors();
    for _ in 0..200 {
        assert!(dir.write(Path::new("locked/file"), b"no", 0o600).is_err());
    }
    assert!(
        open_descriptors() <= baseline + noise,
        "a failed temporary creation must not retain a descriptor"
    );
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
}

/// Mirrors `tests/test_adapter_home.py::AdapterHomeTests::test_failed_atomic_replace_preserves_existing_content`
#[test]
fn failed_atomic_replace_preserves_existing_content() {
    let root = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    dir.write(Path::new("settings/config.toml"), b"original", 0o600)
        .unwrap();

    let fault = fail_at(fs::FaultPoint::BeforeRename);
    assert!(dir
        .write_seamed(
            Path::new("settings/config.toml"),
            b"replacement",
            0o600,
            Some(&fault)
        )
        .is_err());
    assert_eq!(
        std::fs::read(root.path().join("settings/config.toml")).unwrap(),
        b"original"
    );

    // A live publication failure (renaming onto a directory) owns its cleanup.
    // The fault injected above simulates a crash, which deliberately leaves its
    // temporary behind, so this half is checked in its own directory.
    dir.directory(Path::new("occupied/target")).unwrap();
    assert!(dir
        .write(Path::new("occupied/target"), b"replacement", 0o600)
        .is_err());
    assert!(
        temporaries(&root.path().join("occupied")).is_empty(),
        "a failed publish removes the temporary it owns"
    );
}

/// Mirrors `tests/test_adapter_home.py::AdapterHomeTests::test_parent_swap_cannot_redirect_managed_replace`
#[test]
fn parent_swap_cannot_redirect_managed_replace() {
    let root = tempdir();
    let outside = tempdir();
    let parent = root.path().join("settings");
    std::fs::create_dir(&parent).unwrap();
    // The retained parent descriptor, not the path, is what publication uses.
    let retained_dir = fs::Dir::open(&parent).unwrap();

    let retained = root.path().join("retained-settings");
    std::fs::rename(&parent, &retained).unwrap();
    std::os::unix::fs::symlink(outside.path(), &parent).unwrap();

    retained_dir
        .write(Path::new("config.toml"), b"retained", 0o600)
        .unwrap();
    assert_eq!(
        std::fs::read(retained.join("config.toml")).unwrap(),
        b"retained"
    );
    assert!(
        !outside.path().join("config.toml").exists(),
        "a swapped-in symlink must never receive the publication"
    );
}

/// Mirrors `tests/test_adapter_home.py::AdapterHomeTests::test_failed_file_sync_publishes_nothing_and_cleans_its_temp`
#[test]
fn failed_publication_publishes_nothing_and_cleans_its_temp() {
    let root = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();

    // Payload failure before the rename: nothing is published under the name.
    let fault = fail_at(fs::FaultPoint::MidWrite);
    assert!(dir
        .write_seamed(Path::new("answer.md"), b"replacement", 0o600, Some(&fault))
        .is_err());
    assert!(!root.path().join("answer.md").exists());

    // A live failure (a parent that cannot accept the temporary) leaves neither
    // a target nor an owned temporary behind.
    dir.directory(Path::new("locked")).unwrap();
    let locked = root.path().join("locked");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o500)).unwrap();
    assert!(dir
        .write(Path::new("locked/answer.md"), b"replacement", 0o600)
        .is_err());
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(!locked.join("answer.md").exists());
    assert!(temporaries(&locked).is_empty());
}

/// Mirrors `tests/test_adapter_home.py::AdapterHomeTests::test_unsupported_directory_sync_does_not_reject_publication`
#[test]
fn unsupported_directory_sync_does_not_reject_publication() {
    // A filesystem that reports directory synchronization unsupported must not
    // fail the publish, while a real sync failure still propagates.
    assert!(
        fs::tolerate_unsupported_sync(Err(std::io::Error::from_raw_os_error(libc::EINVAL))).is_ok()
    );
    assert!(
        fs::tolerate_unsupported_sync(Err(std::io::Error::from_raw_os_error(libc::ENOTSUP)))
            .is_ok()
    );
    assert!(
        fs::tolerate_unsupported_sync(Err(std::io::Error::from_raw_os_error(libc::EIO))).is_err()
    );
    assert!(fs::tolerate_unsupported_sync(Ok(())).is_ok());

    let root = tempdir();
    let dir = fs::Dir::open(root.path()).unwrap();
    dir.write(Path::new("answer.md"), b"replacement", 0o600)
        .unwrap();
    assert_eq!(
        std::fs::read(root.path().join("answer.md")).unwrap(),
        b"replacement"
    );
}
