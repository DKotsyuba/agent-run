//! Resolved paths below the private agent-run home, matching `paths.py`.
use crate::fs;
use agent_run_domain::{domain::AgentId, error::invalid, Result};
use std::{
    os::unix::fs::PermissionsExt,
    path::{Component, Path, PathBuf},
    str::FromStr,
};

/// Root of the private agent-run home. Delegates to [`fs::home`], which
/// applies the same `AGENT_RUN_HOME`/`~` expansion and canonicalizes an
/// existing tree (`paths.py:agent_run_home`).
pub fn agent_run_home(value: Option<PathBuf>) -> Result<PathBuf> {
    fs::home(value)
}

/// `<home>/config.toml` (`paths.py:config_path`).
pub fn config_path(home: Option<PathBuf>) -> Result<PathBuf> {
    Ok(agent_run_home(home)?.join("config.toml"))
}

/// `<home>/state.db` (`paths.py:state_db_path`).
pub fn state_db_path(home: Option<PathBuf>) -> Result<PathBuf> {
    Ok(agent_run_home(home)?.join("state.db"))
}

/// Collapse `.`/`..` components lexically, without touching the filesystem.
/// `candidate` need not exist, so this is not full symlink-resolving
/// canonicalization; it only defeats `..` segments appended after an
/// (already-canonical) `root`, the same escape `paths.py`'s `Path.resolve()`
/// closes for a not-yet-existing subtree before `_require_beneath` checks it.
fn normalize(path: &Path) -> PathBuf {
    let mut stack: Vec<Component> = Vec::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(stack.last(), Some(Component::Normal(_))) {
                    stack.pop();
                } else {
                    stack.push(component);
                }
            }
            other => stack.push(other),
        }
    }
    stack.iter().collect()
}

/// Reject `candidate` unless it stays lexically beneath `root`
/// (`paths.py:_require_beneath`).
fn require_beneath(root: &Path, candidate: &Path) -> Result<()> {
    if !normalize(candidate).starts_with(normalize(root)) {
        return Err(invalid(format!(
            "path escapes agent-run home: {}",
            candidate.display()
        )));
    }
    Ok(())
}

/// The directory owned by one agent id: `<home>/agents/<agent_id>`
/// (`paths.py:agent_dir`, which validates through `validate_agent_id`
/// first). Rejecting anything but a well-formed `AgentId` up front, before
/// the join, is what makes this a private per-agent directory rather than a
/// generic freeform path; its fixed `ag-YYYYMMDD-HHMMSS-<hex10>` shape also
/// contains no path separators, so [`require_beneath`] below can never
/// actually fire for it, unlike [`runtime_skills_dir`]'s freeform component.
pub fn agent_dir(agent_id: &str, home: Option<PathBuf>) -> Result<PathBuf> {
    let id = AgentId::from_str(agent_id)?;
    let root = agent_run_home(home)?;
    let agents_root = root.join("agents");
    require_beneath(&root, &agents_root)?;
    let candidate = agents_root.join(id.as_str());
    require_beneath(&agents_root, &candidate)?;
    Ok(candidate)
}

/// Create and return one agent's private directory, mode `0700` on every
/// ancestor this call creates and on the directory itself
/// (`paths.py:create_agent_dir`). An existing non-directory or symlink at any
/// level is refused rather than silently reused or followed.
pub fn create_agent_dir(agent_id: &str, home: Option<PathBuf>) -> Result<PathBuf> {
    let candidate = agent_dir(agent_id, home)?;
    if let Some(parent) = candidate.parent() {
        create_private_ancestors(parent)?;
    }
    match std::fs::symlink_metadata(&candidate) {
        Ok(m) if m.file_type().is_symlink() => {
            return Err(invalid("agent directory must not be a symlink"));
        }
        Ok(m) if m.is_dir() => {}
        Ok(_) => return Err(invalid("agent directory path is not a directory")),
        Err(_) => std::fs::create_dir(&candidate)?,
    }
    std::fs::set_permissions(&candidate, std::fs::Permissions::from_mode(0o700))?;
    Ok(candidate)
}

/// Ensure one ancestor directory chain exists, private (`0700`) on every
/// level newly created by this call. An already-existing ancestor is left at
/// its current mode, matching `Path.mkdir(parents=True, exist_ok=True)`.
fn create_private_ancestors(dir: &Path) -> Result<()> {
    match std::fs::symlink_metadata(dir) {
        Ok(m) if m.file_type().is_symlink() => {
            Err(invalid("agent-run home path must not be a symlink"))
        }
        Ok(m) if m.is_dir() => Ok(()),
        Ok(_) => Err(invalid("agent-run home path is not a directory")),
        Err(_) => {
            if let Some(parent) = dir.parent() {
                create_private_ancestors(parent)?;
            }
            std::fs::create_dir(dir)?;
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
            Ok(())
        }
    }
}

/// `<home>/skills/<runtime>` (`paths.py:runtime_skills_dir`). `runtime` is
/// free text, so unlike [`agent_dir`] the escape check happens after joining.
pub fn runtime_skills_dir(runtime: &str, home: Option<PathBuf>) -> Result<PathBuf> {
    if runtime.trim().is_empty() {
        return Err(invalid("runtime must be a nonblank string"));
    }
    let root = agent_run_home(home)?;
    let skills_root = root.join("skills");
    require_beneath(&root, &skills_root)?;
    let candidate = skills_root.join(runtime);
    require_beneath(&skills_root, &candidate)?;
    Ok(candidate)
}
