use crate::{
    config::{self, Config, Runtime},
    policy::Constraint,
};
use agent_run_domain::{
    domain::{self, StartRequest},
    error::invalid,
    Result,
};
use agent_run_platform::fs;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf};

/// Return whether a profile basename is a safe configured identifier.
fn valid_profile_name(value: &str) -> bool {
    !value.is_empty()
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    pub name: String,
    pub body: String,
    pub write: bool,
    pub network: bool,
    pub revision: String,
    pub canonical: bool,
    pub allow_external_read_roots: bool,
    pub read_roots: Vec<PathBuf>,
    pub skills: Vec<String>,
    pub mcp: Vec<String>,
    pub required_constraints: BTreeSet<Constraint>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Meta {
    revision: Option<String>,
    write: Option<bool>,
    network: Option<bool>,
    allow_external_read_roots: Option<bool>,
    skills: Option<Vec<String>>,
    mcp: Option<Vec<String>>,
    required_constraints: Option<Vec<Constraint>>,
}
pub fn normalize_roots(roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut a = roots.to_vec();
    a.sort_by_key(|p| (p.components().count(), p.clone()));
    a.dedup();
    let mut b: Vec<PathBuf> = Vec::new();
    for p in a {
        if !b.iter().any(|q| p.starts_with(q)) {
            b.push(p);
        }
    }
    b
}

/// Resolve existing absolute directories and retain only the minimal antichain.
///
/// Symlink aliases are canonicalized before deduplication. Relative, missing,
/// non-directory, and otherwise unresolvable inputs are rejected so a profile
/// never grants a path whose ownership cannot be proven.
pub fn normalize_read_roots(roots: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut resolved = Vec::with_capacity(roots.len());
    for root in roots {
        if !root.is_absolute() {
            return Err(invalid("read root must be an absolute existing directory"));
        }
        let path = root
            .canonicalize()
            .map_err(|_| invalid("read root must be an absolute existing directory"))?;
        if !path.is_dir() {
            return Err(invalid("read root must be an absolute existing directory"));
        }
        resolved.push(path);
    }
    Ok(normalize_roots(&resolved))
}

/// Resolve one configured profile file without following an escaping link.
pub fn profile_path(directory: &std::path::Path, name: &str) -> Result<PathBuf> {
    if !valid_profile_name(name) {
        return Err(invalid(
            "profile must be a configured profile name, not a path",
        ));
    }
    if !directory.is_absolute() {
        return Err(invalid(
            "profiles.directory must be an absolute existing directory",
        ));
    }
    let root = directory
        .canonicalize()
        .map_err(|_| invalid("profiles.directory must be an absolute existing directory"))?;
    if !root.is_dir() {
        return Err(invalid(
            "profiles.directory must be an absolute existing directory",
        ));
    }
    let candidate = root.join(format!("{name}.md"));
    let resolved = candidate
        .canonicalize()
        .map_err(|_| invalid(format!("profile does not exist: {name}")))?;
    if !resolved.starts_with(&root) {
        return Err(agent_run_domain::Error::PathEscape(format!(
            "profile escapes configured directory: {name}"
        )));
    }
    if !resolved.is_file() {
        return Err(invalid(format!("profile is not a file: {name}")));
    }
    Ok(resolved)
}
pub fn parse(text: &str, request: &StartRequest) -> Result<Profile> {
    let (meta, body) = if let Some(rest) = text.strip_prefix("+++\n") {
        let (front, body) = rest
            .split_once("\n+++\n")
            .ok_or_else(|| invalid("unterminated profile front matter"))?;
        let meta: Meta = toml::from_str(front).map_err(|_| invalid("invalid profile metadata"))?;
        (meta, body)
    } else {
        (
            Meta {
                revision: None,
                write: None,
                network: None,
                allow_external_read_roots: None,
                skills: None,
                mcp: None,
                required_constraints: None,
            },
            text,
        )
    };
    domain::nonblank("profile body", body)?;
    let canonical = meta.revision.is_some();
    if canonical
        && (meta.write.is_none()
            || meta.network.is_none()
            || meta.allow_external_read_roots.is_none()
            || meta.skills.is_none()
            || meta.mcp.is_none()
            || meta.required_constraints.is_none())
    {
        return Err(invalid("canonical role is incomplete"));
    }
    if !canonical
        && (meta.allow_external_read_roots.is_some()
            || meta.skills.is_some()
            || meta.mcp.is_some()
            || meta.required_constraints.is_some())
    {
        return Err(invalid("canonical profile fields require revision"));
    }
    let required_vec = meta.required_constraints.unwrap_or_default();
    let mut required_constraints: BTreeSet<_> = required_vec.iter().copied().collect();
    if required_vec.len() != required_constraints.len() {
        return Err(invalid("duplicate required constraints"));
    }
    // Explicit caller requirements can narrow but must never be silently discarded.
    required_constraints.extend(request.required_constraints.iter().copied());
    let allow_external_read_roots = meta.allow_external_read_roots.unwrap_or(true);
    if !request.read_roots.is_empty() && !allow_external_read_roots {
        return Err(invalid("role forbids external read roots"));
    }
    let revision = meta.revision.unwrap_or_else(|| "legacy".into());
    domain::nonblank("profile revision", &revision)?;
    Ok(Profile {
        name: request.profile.clone(),
        body: body.trim().into(),
        write: meta.write.unwrap_or(false) && (canonical || request.write),
        network: meta.network.unwrap_or(false),
        revision,
        canonical,
        allow_external_read_roots,
        read_roots: normalize_read_roots(&request.read_roots)?,
        skills: meta.skills.unwrap_or_default(),
        mcp: meta.mcp.unwrap_or_default(),
        required_constraints,
    })
}
pub fn load(cfg: &Config, rt: &Runtime, request: &StartRequest) -> Result<Profile> {
    if !config::name(&request.profile) || request.profile.contains('.') {
        return Err(invalid("profile must be a configured name, not a path"));
    }
    let raw = fs::Dir::open(cfg.profiles_dir())?.read(
        profile_path(cfg.profiles_dir(), &request.profile)?
            .strip_prefix(cfg.profiles_dir())
            .map_err(|_| invalid("profile escapes configured directory"))?,
        1024 * 1024,
    )?;
    let mut p = parse(
        std::str::from_utf8(&raw).map_err(|_| invalid("profile must be UTF-8"))?,
        request,
    )?;
    if p.canonical && (!rt.skills.is_empty() || !rt.mcp.is_empty()) {
        return Err(invalid(
            "canonical role cannot mix runtime skills or MCP declarations",
        ));
    }
    if !p.canonical {
        p.skills = rt.skills.clone();
        p.mcp = rt.mcp.clone();
    }
    for list in [&p.skills, &p.mcp] {
        let set: BTreeSet<_> = list.iter().collect();
        if set.len() != list.len() || list.iter().any(|v| !config::name(v)) {
            return Err(invalid("profile skills/MCP must be unique catalog names"));
        }
    }
    if p.mcp.iter().any(|n| !cfg.mcp.contains_key(n)) {
        return Err(invalid("profile refers to unknown MCP server"));
    }
    Ok(p)
}
