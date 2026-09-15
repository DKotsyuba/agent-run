//! Durable publication and fault-injection seams (`src/publish.rs`).
use agent_run_platform::{
    fs::{Dir, FaultPoint},
    publish::{self, Entry},
};
use std::path::Path;

fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("agent-run-publish-")
        .tempdir_in(std::env::var_os("AGENT_RUN_TEST_TMP").unwrap_or_else(|| "/tmp".into()))
        .expect("tempdir")
}

fn fail_at(target: FaultPoint) -> impl Fn(FaultPoint) -> agent_run_domain::Result<()> {
    move |point| {
        if point == target {
            Err(agent_run_domain::error::invalid("simulated crash"))
        } else {
            Ok(())
        }
    }
}

/// Mirrors `seal_answer`'s marker/payload/proof ordering (adapters/home.py):
/// entries publish in the given order, each visible only once fully written.
#[test]
fn publish_group_writes_entries_in_order() {
    let root = tempdir();
    let dir = Dir::open(root.path()).unwrap();
    publish::publish_group(
        &dir,
        &[
            Entry::new(Path::new(".marker"), b"2\n", 0o600),
            Entry::new(Path::new("answer.md"), b"hello", 0o600),
            Entry::new(Path::new("answer.md.proof.json"), b"{}", 0o600),
        ],
    )
    .unwrap();
    assert_eq!(dir.read(Path::new(".marker"), 16).unwrap(), b"2\n");
    assert_eq!(dir.read(Path::new("answer.md"), 16).unwrap(), b"hello");
    assert_eq!(
        dir.read(Path::new("answer.md.proof.json"), 16).unwrap(),
        b"{}"
    );
}

/// A later entry failing must not roll back or touch earlier, already
/// durably published entries (no cross-file transaction, per plan 9.2).
#[test]
fn publish_group_leaves_earlier_entries_durable_when_a_later_one_fails() {
    let root = tempdir();
    let dir = Dir::open(root.path()).unwrap();
    // The second entry's path is unwritable: its parent directory does not
    // exist and cannot be created because "blocked" is a plain file, not a
    // directory, so `mkdirat` fails before any bytes for that entry are touched.
    dir.write(Path::new("blocked"), b"x", 0o600).unwrap();
    let result = publish::publish_group(
        &dir,
        &[
            Entry::new(Path::new("first"), b"one", 0o600),
            Entry::new(Path::new("blocked/second"), b"two", 0o600),
            Entry::new(Path::new("third"), b"three", 0o600),
        ],
    );
    assert!(result.is_err());
    assert_eq!(dir.read(Path::new("first"), 16).unwrap(), b"one");
    assert_eq!(dir.optional(Path::new("third"), 16).unwrap(), None);
}

/// Fault at `MidWrite` simulates the process dying right after the temp
/// file's bytes are written, before they are synced or renamed. A real crash
/// there never runs our cleanup code either, so the temp file is left behind
/// (holding the full payload, since `write_all` had already completed) while
/// the published name stays exactly as it was before the call (here, absent).
#[test]
fn fault_mid_write_leaves_the_published_name_untouched() {
    let root = tempdir();
    let dir = Dir::open(root.path()).unwrap();
    let fault = fail_at(FaultPoint::MidWrite);
    let result =
        publish::publish_file_with_fault(&dir, Path::new("out"), b"payload", 0o600, &fault);
    assert!(result.is_err());
    assert_eq!(dir.optional(Path::new("out"), 1024).unwrap(), None);
    let temp_files: Vec<_> = std::fs::read_dir(root.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let n = e.file_name();
            let n = n.to_string_lossy();
            n.starts_with(".agent-run-") && n.ends_with(".tmp")
        })
        .collect();
    assert_eq!(
        temp_files.len(),
        1,
        "a real crash would not run cleanup either"
    );
    assert_eq!(std::fs::read(temp_files[0].path()).unwrap(), b"payload");
}

/// Fault at `MidWrite` over an existing file: the crash must not touch the
/// previously published content.
#[test]
fn fault_mid_write_preserves_the_previous_published_content() {
    let root = tempdir();
    let dir = Dir::open(root.path()).unwrap();
    dir.write(Path::new("out"), b"original", 0o600).unwrap();
    let fault = fail_at(FaultPoint::MidWrite);
    let result =
        publish::publish_file_with_fault(&dir, Path::new("out"), b"replacement", 0o600, &fault);
    assert!(result.is_err());
    assert_eq!(dir.read(Path::new("out"), 1024).unwrap(), b"original");
}

/// Fault at `BeforeRename`: the temp file is fully written and synced but
/// never renamed, so the published name is still untouched, and the crash
/// leaves a recoverable, distinctly named temp file behind rather than
/// silently discarding evidence of the interrupted attempt.
#[test]
fn fault_before_rename_leaves_the_published_name_untouched_and_temp_recoverable() {
    let root = tempdir();
    let dir = Dir::open(root.path()).unwrap();
    let fault = fail_at(FaultPoint::BeforeRename);
    let result =
        publish::publish_file_with_fault(&dir, Path::new("out"), b"payload", 0o600, &fault);
    assert!(result.is_err());
    assert_eq!(dir.optional(Path::new("out"), 1024).unwrap(), None);
    let temp_files: Vec<_> = std::fs::read_dir(root.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".agent-run-") && n.ends_with(".tmp"))
        .collect();
    assert_eq!(
        temp_files.len(),
        1,
        "the synced temp file must remain for recovery"
    );
}

/// Fault at `AfterRename`: the rename already committed, so the published
/// name must hold the full new bytes even though the parent-directory sync
/// (the only step skipped) never ran. The call still reports failure, so a
/// caller cannot mistake this for a clean durable publish.
#[test]
fn fault_after_rename_still_exposes_the_full_new_content() {
    let root = tempdir();
    let dir = Dir::open(root.path()).unwrap();
    let fault = fail_at(FaultPoint::AfterRename);
    let result =
        publish::publish_file_with_fault(&dir, Path::new("out"), b"payload", 0o600, &fault);
    assert!(result.is_err(), "the caller must see an explicit failure");
    assert_eq!(
        dir.read(Path::new("out"), 1024).unwrap(),
        b"payload",
        "rename already committed the full bytes"
    );
}

/// No fault at all: the file is durably published and no temp litter remains,
/// the baseline every fault case above is measured against.
#[test]
fn no_fault_publishes_cleanly_with_no_leftovers() {
    let root = tempdir();
    let dir = Dir::open(root.path()).unwrap();
    let no_fault = |_: FaultPoint| Ok(());
    publish::publish_file_with_fault(&dir, Path::new("out"), b"payload", 0o600, &no_fault).unwrap();
    assert_eq!(dir.read(Path::new("out"), 1024).unwrap(), b"payload");
    let leftovers = std::fs::read_dir(root.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n != "out")
        .count();
    assert_eq!(leftovers, 0);
}
