use crate::{
    config::{self, Config, Runtime},
    domain::StartRequest,
    error::invalid,
    fs,
    policy::Constraint,
    Result,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf};
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
    crate::domain::nonblank("profile body", body)?;
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
    crate::domain::nonblank("profile revision", &revision)?;
    Ok(Profile {
        name: request.profile.clone(),
        body: body.trim().into(),
        write: meta.write.unwrap_or(false) && (canonical || request.write),
        network: meta.network.unwrap_or(false),
        revision,
        canonical,
        allow_external_read_roots,
        read_roots: normalize_roots(&request.read_roots),
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
        std::path::Path::new(&format!("{}.md", request.profile)),
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
