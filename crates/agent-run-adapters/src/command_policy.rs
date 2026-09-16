//! Owner-configured command denials and deterministic native rule renderers.

use agent_run_domain::{error::invalid, Result};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

/// Ownership metadata filename written inside each managed refusal directory.
const MARKER: &str = ".agent-run-command-policy.json";
/// Exact executable shell body used for every denied ordinary invocation.
const REFUSAL: &str =
    "#!/bin/sh\nprintf '%s\\n' 'agent-run: command denied by owner policy' >&2\nexit 126\n";

/// A refreshed refusal directory and the first executable found for each name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedCommandPolicy {
    /// Private directory intended to lead the child's ordinary `PATH`.
    pub directory: PathBuf,
    /// Bare denied names mapped to their first executable path, when found.
    pub resolved_commands: BTreeMap<String, PathBuf>,
}

/// Validate and deterministically deduplicate bare command names.
pub fn validate_denied_commands(commands: &[String]) -> Result<Vec<String>> {
    let mut names = BTreeSet::new();
    for command in commands {
        let bytes = command.as_bytes();
        if bytes.is_empty()
            || !bytes[0].is_ascii_alphanumeric() && bytes[0] != b'_'
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_.+-".contains(byte))
        {
            return Err(invalid("denied command must be a bare command name"));
        }
        names.insert(command.clone());
    }
    Ok(names.into_iter().collect())
}

/// Safely refresh owned refusal shims without replacing user-modified entries.
pub fn materialize_refusal_commands(
    commands: &[String],
    directory: &Path,
    search_paths: &[PathBuf],
) -> Result<MaterializedCommandPolicy> {
    let denied = validate_denied_commands(commands)?;
    prepare_directory(directory)?;
    let old = read_marker(directory)?;
    for command in old {
        if !denied.contains(&command) {
            remove_refusal(&directory.join(command))?;
        }
    }
    for command in &denied {
        write_refusal(&directory.join(command))?;
    }
    write_marker(directory, &denied)?;
    let mut resolved = BTreeMap::new();
    for command in &denied {
        if let Some(path) = search_paths.iter().find_map(|root| {
            let candidate = root.join(command);
            candidate
                .metadata()
                .ok()
                .filter(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
                .map(|_| candidate)
        }) {
            resolved.insert(command.clone(), path);
        }
    }
    Ok(MaterializedCommandPolicy {
        directory: directory.into(),
        resolved_commands: resolved,
    })
}

/// Render Codex forbidden rules for bare names, absolute aliases, and targets.
pub fn render_codex_denial_rules(commands: &[String], command_paths: &[PathBuf]) -> Result<String> {
    render_codex_rules(
        commands,
        command_paths,
        "forbidden",
        "Denied by owner command policy",
    )
}

/// Render Codex prompt rules for bare names and resolved executable forms.
pub fn render_codex_review_rules(
    commands: &[String],
    environment: &BTreeMap<String, String>,
) -> Result<String> {
    let names = validate_denied_commands(commands)?;
    let paths = resolve_commands(
        &names,
        environment.get("PATH").map(String::as_str).unwrap_or(""),
    );
    render_codex_rules(
        &names,
        &paths,
        "prompt",
        "Review network command before execution",
    )
}

/// Render exact-command Bash denials for Claude- or Qwen-shaped settings.
pub fn render_bash_denials(commands: &[String], command_paths: &[PathBuf]) -> Result<Vec<String>> {
    Ok(native_patterns(commands, command_paths)?
        .into_iter()
        .flat_map(|pattern| [format!("Bash({pattern})"), format!("Bash({pattern} *)")])
        .collect())
}

/// Resolve the first executable in a colon-separated path using absolute roots only.
fn resolve_commands(commands: &[String], path: &str) -> Vec<PathBuf> {
    commands
        .iter()
        .filter_map(|command| {
            path.split(':')
                .filter(|part| !part.is_empty())
                .find_map(|part| {
                    let root = Path::new(part);
                    let candidate = root.join(command);
                    (root.is_absolute()
                        && candidate.metadata().is_ok_and(|meta| {
                            meta.is_file() && meta.permissions().mode() & 0o111 != 0
                        }))
                    .then_some(candidate)
                })
        })
        .collect()
}

/// Render one deterministic Codex rule per unique native command pattern.
fn render_codex_rules(
    commands: &[String],
    command_paths: &[PathBuf],
    decision: &str,
    justification: &str,
) -> Result<String> {
    let patterns = native_patterns(commands, command_paths)?;
    Ok(patterns
        .into_iter()
        .map(|pattern| {
            format!(
                "prefix_rule(pattern=[{}], decision=\"{decision}\", justification=\"{justification}\")\n",
                serde_json::to_string(&pattern).expect("string JSON serialization")
            )
        })
        .collect())
}

/// Return bare names, absolute paths, and canonical symlink targets.
fn native_patterns(commands: &[String], command_paths: &[PathBuf]) -> Result<BTreeSet<String>> {
    let mut patterns: BTreeSet<String> = validate_denied_commands(commands)?.into_iter().collect();
    for path in command_paths {
        if !path.is_absolute() {
            return Err(invalid("native command path must be absolute"));
        }
        patterns.insert(path.to_string_lossy().into_owned());
        patterns.insert(path.canonicalize()?.to_string_lossy().into_owned());
    }
    Ok(patterns)
}

/// Verify or create the private policy directory without following a link.
fn prepare_directory(directory: &Path) -> Result<()> {
    match fs::symlink_metadata(directory) {
        Ok(meta) if meta.file_type().is_symlink() || !meta.is_dir() => {
            Err(invalid("command-policy directory must be a real directory"))
        }
        Ok(_) => {
            let marker = directory.join(MARKER);
            match fs::symlink_metadata(marker) {
                Ok(meta) if meta.is_file() && !meta.file_type().is_symlink() => Ok(()),
                Ok(meta) if meta.file_type().is_symlink() => {
                    Err(invalid("command-policy marker must be a regular file"))
                }
                _ => Err(invalid(
                    "command-policy directory is not managed by agent-run",
                )),
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(directory)?;
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

/// Read and validate the owned marker as a regular file.
fn read_marker(directory: &Path) -> Result<Vec<String>> {
    let marker = directory.join(MARKER);
    let meta = match fs::symlink_metadata(&marker) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    if !meta.is_file() || meta.file_type().is_symlink() {
        return Err(invalid("command-policy marker must be a regular file"));
    }
    let document: serde_json::Value = serde_json::from_slice(&fs::read(marker)?)
        .map_err(|_| invalid("command-policy marker is invalid"))?;
    if document.get("version") != Some(&json!(1)) {
        return Err(invalid("command-policy marker is invalid"));
    }
    let values = document
        .get("commands")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| invalid("command-policy marker is invalid"))?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid("command-policy marker is invalid"))
        })
        .collect::<Result<Vec<_>>>()
        .and_then(|values| validate_denied_commands(&values))
}

/// Atomically replace the marker with canonical JSON.
fn write_marker(directory: &Path, commands: &[String]) -> Result<()> {
    atomic_write(
        &directory.join(MARKER),
        format!(
            "{{\"version\": 1, \"commands\": {}}}\n",
            serde_json::to_string(commands)?
        )
        .as_bytes(),
        0o600,
    )
}

/// Atomically create or replace one verified refusal shim.
fn write_refusal(path: &Path) -> Result<()> {
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.file_type().is_symlink() || !meta.is_file() || fs::read_to_string(path)? != REFUSAL
        {
            return Err(invalid(format!(
                "refusing to replace unmanaged command-policy entry: {}",
                path.display()
            )));
        }
    }
    atomic_write(path, REFUSAL.as_bytes(), 0o700)
}

/// Remove one unchanged regular refusal shim and nothing else.
fn remove_refusal(path: &Path) -> Result<()> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if meta.file_type().is_symlink() || !meta.is_file() || fs::read_to_string(path)? != REFUSAL {
        return Err(invalid(format!(
            "refusing to remove unmanaged command-policy entry: {}",
            path.display()
        )));
    }
    fs::remove_file(path)?;
    Ok(())
}

/// Replace a sibling file without following the destination when renaming.
fn atomic_write(path: &Path, content: &[u8], mode: u32) -> Result<()> {
    let temporary = path.with_file_name(format!(
        ".{}.new",
        path.file_name().unwrap().to_string_lossy()
    ));
    if temporary.exists() || temporary.symlink_metadata().is_ok() {
        return Err(invalid("command-policy temporary already exists"));
    }
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(&temporary)?;
    file.write_all(content)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temporary, path)?;
    Ok(())
}
