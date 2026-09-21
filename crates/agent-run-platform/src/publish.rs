//! Durable multi-file publication built on [`Dir`]'s atomic single-file
//! writes (`fs.rs`). Group publication has no cross-file transaction: each
//! entry is its own ordered, durable temp-then-rename replacement, exactly
//! as `adapters/home.py`'s `write_managed_file` documents for `seal_answer`'s
//! marker/payload/proof sequence and `snapshot_tree.py`'s manifest-last
//! publish (see `docs/artifact-snapshots.md`).
use crate::fs::Dir;
pub use crate::fs::FaultPoint;
use agent_run_domain::Result;
use std::path::Path;

/// One durable entry in a [`publish_group`] call: relative path, exact
/// bytes, and permission mode (`0o600` private data, `0o700` private
/// executable, matching `write_managed_file`'s accepted modes).
pub struct Entry<'a> {
    pub path: &'a Path,
    pub data: &'a [u8],
    pub mode: u32,
}
impl<'a> Entry<'a> {
    pub fn new(path: &'a Path, data: &'a [u8], mode: u32) -> Self {
        Self { path, data, mode }
    }
}

/// Publish `entries` in the given order beneath `dir`. Each entry is its own
/// atomic write (temp file, fsync, rename, parent fsync); order is the only
/// guarantee across entries, so callers list a commit marker first and any
/// manifest/proof last, the same visibility contract as `write_managed_file`
/// and `_publish_snapshot` (adapters/home.py, adapters/snapshot_tree.py). A
/// failure on entry N leaves entries `0..N` durably published and N's real
/// name untouched or fully replaced, never partial; entries after N are not
/// attempted.
pub fn publish_group(dir: &Dir, entries: &[Entry<'_>]) -> Result<()> {
    for entry in entries {
        dir.write(entry.path, entry.data, entry.mode)?;
    }
    Ok(())
}

/// Test-only seam over [`Dir::write_seamed`]: publish one entry with a fault
/// hook that can fail at [`FaultPoint::MidWrite`], [`FaultPoint::BeforeRename`],
/// or [`FaultPoint::AfterRename`]. No production call site uses this; they all
/// go through [`publish_group`]/[`Dir::write`], which never install a hook.
pub fn publish_file_with_fault(
    dir: &Dir,
    path: &Path,
    data: &[u8],
    mode: u32,
    fault: &dyn Fn(FaultPoint) -> Result<()>,
) -> Result<()> {
    dir.write_seamed(path, data, mode, Some(fault))
}
