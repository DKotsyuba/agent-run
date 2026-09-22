//! Runtime-neutral resolution of canonical role contracts.
//!
//! Ports `src/agent_run/role_plan.py`: `ResolvedSkill`, `ResolvedMcp`,
//! `ResolvedRolePlan` (canonical payload, `to_payload`/`from_payload`), and
//! `resolve_role_plan`. The frozen plan never carries credential bytes,
//! argv, process environment, or generated-home paths; `config_revision`
//! hashes the full credential-free payload with
//! `agent_run_domain::canonical` (byte-exact CPython `json.dumps` plus
//! sha256), so identical inputs produce an identical, Python-compatible hash
//! for every runtime.
use crate::{
    config::Mcp,
    policy::{self, Constraint},
    profiles::{self, Profile},
};
use agent_run_domain::{
    canonical, catalog::ResolvedLaunchAuthority, error::invalid, Result, Sha256Digest,
};
use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
};

/// One canonical skill identity and content revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSkill {
    pub id: String,
    pub revision: String,
}

/// One credential-free MCP definition selected by a role: canonical launch
/// data plus an `env_from` list of names (never values) and the
/// operator-selected native approval mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedMcp {
    pub id: String,
    pub transport: String,
    pub command: String,
    pub args: Vec<String>,
    pub env_from: Vec<String>,
    pub approval_mode: String,
}

/// Immutable, serializable role contract shared by every adapter. Never
/// contains credential bytes, argv, process environment, generated-home
/// paths, or an adapter launch plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRolePlan {
    pub role_name: String,
    pub role_revision: String,
    pub prompt: String,
    pub write: bool,
    pub network: bool,
    pub allow_external_read_roots: bool,
    pub read_roots: Vec<PathBuf>,
    pub skills: Vec<ResolvedSkill>,
    pub mcp: Vec<ResolvedMcp>,
    pub required_constraints: BTreeSet<Constraint>,
    pub auth_mode: String,
    pub auth_reference: Option<String>,
    pub config_revision: String,
}

const APPROVAL_MODES: [&str; 4] = ["auto", "prompt", "writes", "approve"];

fn sorted_constraint_names(constraints: &BTreeSet<Constraint>) -> Vec<String> {
    let mut names: Vec<String> = constraints
        .iter()
        .map(|constraint| policy::constraint_name(*constraint))
        .collect();
    names.sort_unstable();
    names
}

/// Build the one canonical role payload, excluding its derived revision.
fn canonical_payload(plan: &ResolvedRolePlan) -> Value {
    let mcp: Vec<Value> = plan
        .mcp
        .iter()
        .map(|server| {
            let mut object = Map::new();
            object.insert("id".into(), json!(server.id));
            object.insert("transport".into(), json!(server.transport));
            object.insert("command".into(), json!(server.command));
            object.insert("args".into(), json!(server.args));
            object.insert("env_from".into(), json!(server.env_from));
            if server.approval_mode != "auto" {
                object.insert("approval_mode".into(), json!(server.approval_mode));
            }
            Value::Object(object)
        })
        .collect();
    json!({
        "role_name": plan.role_name,
        "role_revision": plan.role_revision,
        "prompt": plan.prompt,
        "grants": {
            "write": plan.write,
            "network": plan.network,
            "allow_external_read_roots": plan.allow_external_read_roots,
            "read_roots": plan.read_roots.iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
        },
        "skills": plan.skills.iter()
            .map(|skill| json!({"id": skill.id, "revision": skill.revision}))
            .collect::<Vec<_>>(),
        "mcp": mcp,
        "required_constraints": sorted_constraint_names(&plan.required_constraints),
        "auth": {"mode": plan.auth_mode, "reference": plan.auth_reference},
    })
}

impl ResolvedRolePlan {
    /// Return the canonical JSON-safe role document without live secrets.
    pub fn to_payload(&self) -> Value {
        let mut payload = canonical_payload(self);
        payload["config_revision"] = json!(self.config_revision);
        payload
    }

    /// Validate and reconstruct one detached JSON role payload.
    ///
    /// Exact object shapes and JSON scalar types are required. Read roots
    /// must be existing absolute directories in normalized antichain order;
    /// IDs, hashes, MCP transport/environment declarations, constraints, and
    /// the auth choice are validated. The credential-free config revision is
    /// recomputed before the immutable plan is returned.
    pub fn from_payload(payload: &Value) -> Result<ResolvedRolePlan> {
        let document = exact_object(
            payload,
            &[
                "role_name",
                "role_revision",
                "prompt",
                "grants",
                "skills",
                "mcp",
                "required_constraints",
                "auth",
                "config_revision",
            ],
            "resolved role",
        )?;

        let grants = exact_object(
            &document["grants"],
            &[
                "write",
                "network",
                "allow_external_read_roots",
                "read_roots",
            ],
            "resolved role grants",
        )?;
        let write = boolean(&grants["write"], "resolved role grants.write")?;
        let network = boolean(&grants["network"], "resolved role grants.network")?;
        let allow_external = boolean(
            &grants["allow_external_read_roots"],
            "resolved role grants.allow_external_read_roots",
        )?;
        let root_values = strings(
            &grants["read_roots"],
            "resolved role grants.read_roots",
            true,
        )?;
        let roots = normalize_and_validate_roots(&root_values)?;
        if !roots.is_empty() && !allow_external {
            return Err(invalid("resolved role does not allow external read roots"));
        }

        let raw_skills = document["skills"]
            .as_array()
            .ok_or_else(|| invalid("resolved role skills must be a list"))?;
        let mut skills = Vec::with_capacity(raw_skills.len());
        for (index, value) in raw_skills.iter().enumerate() {
            let item = exact_object(
                value,
                &["id", "revision"],
                &format!("resolved role skills[{index}]"),
            )?;
            let id = text(
                &item["id"],
                &format!("resolved role skills[{index}].id"),
                false,
            )?;
            let revision = text(
                &item["revision"],
                &format!("resolved role skills[{index}].revision"),
                false,
            )?;
            if !config_id(id) || !is_sha256_hex(revision) {
                return Err(invalid(format!("resolved role skills[{index}] is invalid")));
            }
            skills.push(ResolvedSkill {
                id: id.to_string(),
                revision: revision.to_string(),
            });
        }
        if unique_len(skills.iter().map(|skill| skill.id.as_str())) != skills.len() {
            return Err(invalid(
                "resolved role skill ids must not contain duplicates",
            ));
        }

        let raw_mcp = document["mcp"]
            .as_array()
            .ok_or_else(|| invalid("resolved role mcp must be a list"))?;
        let mut servers = Vec::with_capacity(raw_mcp.len());
        for (index, value) in raw_mcp.iter().enumerate() {
            let object = value.as_object().ok_or_else(|| {
                invalid(format!("resolved role mcp[{index}] has an invalid shape"))
            })?;
            let keys: BTreeSet<&str> = object.keys().map(String::as_str).collect();
            let old_keys: BTreeSet<&str> =
                ["id", "transport", "command", "args", "env_from"].into();
            let mut new_keys = old_keys.clone();
            new_keys.insert("approval_mode");
            if keys != old_keys && keys != new_keys {
                return Err(invalid(format!(
                    "resolved role mcp[{index}] has an invalid shape"
                )));
            }
            let id = text(
                &object["id"],
                &format!("resolved role mcp[{index}].id"),
                false,
            )?;
            let transport = text(
                &object["transport"],
                &format!("resolved role mcp[{index}].transport"),
                false,
            )?;
            let command = text(
                &object["command"],
                &format!("resolved role mcp[{index}].command"),
                false,
            )?;
            let args = strings(
                &object["args"],
                &format!("resolved role mcp[{index}].args"),
                false,
            )?;
            let env_from = strings(
                &object["env_from"],
                &format!("resolved role mcp[{index}].env_from"),
                true,
            )?;
            let approval_mode = match object.get("approval_mode") {
                Some(value) => text(
                    value,
                    &format!("resolved role mcp[{index}].approval_mode"),
                    false,
                )?
                .to_string(),
                None => "auto".to_string(),
            };
            let canonical_command = lexical_absolute(command)?;
            if !config_id(id)
                || transport != "stdio"
                || command != canonical_command
                || env_from.iter().any(|name| !crate::config::env_name(name))
                || !APPROVAL_MODES.contains(&approval_mode.as_str())
            {
                return Err(invalid(format!("resolved role mcp[{index}] is invalid")));
            }
            servers.push(ResolvedMcp {
                id: id.to_string(),
                transport: transport.to_string(),
                command: command.to_string(),
                args,
                env_from,
                approval_mode,
            });
        }
        if unique_len(servers.iter().map(|server| server.id.as_str())) != servers.len() {
            return Err(invalid("resolved role MCP ids must not contain duplicates"));
        }

        let constraint_values = strings(
            &document["required_constraints"],
            "resolved role required_constraints",
            false,
        )?;
        let mut constraints = BTreeSet::new();
        for value in &constraint_values {
            let constraint = policy::constraint_from_name(value)
                .ok_or_else(|| invalid("resolved role contains an unknown constraint"))?;
            constraints.insert(constraint);
        }
        if sorted_constraint_names(&constraints) != constraint_values {
            return Err(invalid(
                "resolved role constraints must be sorted and unique",
            ));
        }

        let auth = exact_object(
            &document["auth"],
            &["mode", "reference"],
            "resolved role auth",
        )?;
        let auth_mode = text(&auth["mode"], "resolved role auth.mode", false)?.to_string();
        let auth_reference = match &auth["reference"] {
            Value::Null => None,
            value => Some(text(value, "resolved role auth.reference", false)?.to_string()),
        };
        if !matches!(auth_mode.as_str(), "global" | "account")
            || (auth_mode == "account") != auth_reference.is_some()
        {
            return Err(invalid("resolved role auth choice is invalid"));
        }
        if let Some(reference) = &auth_reference {
            if !crate::config::account(reference) {
                return Err(invalid("resolved role auth reference is invalid"));
            }
        }

        let role_name = text(&document["role_name"], "resolved role role_name", false)?.to_string();
        let role_revision = text(
            &document["role_revision"],
            "resolved role role_revision",
            false,
        )?
        .to_string();
        let prompt = text(&document["prompt"], "resolved role prompt", false)?.to_string();
        let config_revision = text(
            &document["config_revision"],
            "resolved role config_revision",
            false,
        )?
        .to_string();
        if !config_id(&role_name) || !is_sha256_hex(&config_revision) {
            return Err(invalid(
                "resolved role identity or config revision is invalid",
            ));
        }

        let mut seed = document.clone();
        seed.remove("config_revision");
        let expected = canonical::sha256_hex(&Value::Object(seed), true);
        if config_revision != expected {
            return Err(invalid(
                "resolved role config revision does not match its payload",
            ));
        }

        let plan = ResolvedRolePlan {
            role_name,
            role_revision,
            prompt,
            write,
            network,
            allow_external_read_roots: allow_external,
            read_roots: roots,
            skills,
            mcp: servers,
            required_constraints: constraints,
            auth_mode,
            auth_reference,
            config_revision,
        };
        if plan.to_payload() != *payload {
            return Err(invalid("resolved role payload is not canonical"));
        }
        Ok(plan)
    }
}

/// Reconstructs the frozen operative role from admitted launch authority.
///
/// Call before every launch or retry with `actual_assets_sha256` computed
/// from the sealed asset bytes, then bind only the current attempt lease for
/// account auth. It rejects digest, role-shape, or read-root validation
/// failures; a changed live profile never supplies replacement grants.
pub fn role_from_authority(
    authority: &ResolvedLaunchAuthority,
    actual_assets_sha256: &Sha256Digest,
) -> Result<ResolvedRolePlan> {
    authority.validate()?;
    if actual_assets_sha256 != &authority.assets_sha256 {
        return Err(invalid("authority tool assets digest mismatch"));
    }
    let plan = ResolvedRolePlan::from_payload(&authority.role_payload)?;
    if plan.role_name != authority.profile {
        return Err(invalid("authority role name differs from profile"));
    }
    Ok(plan)
}

/// Resolve one canonical profile against shared skill and MCP catalogs.
///
/// Missing or unsafe skills, missing MCP definitions, disallowed task read
/// roots, and inconsistent auth choices are rejected. The returned revision
/// hashes the full credential-free payload, so identical inputs produce
/// identical role plans for every runtime.
pub fn resolve_role_plan(
    profile: &Profile,
    skills_root: &Path,
    mcp_catalog: &BTreeMap<String, Mcp>,
    auth_mode: &str,
    auth_reference: Option<&str>,
) -> Result<ResolvedRolePlan> {
    if !profile.canonical {
        return Err(invalid("resolved role plans require a canonical profile"));
    }
    if !skills_root.is_absolute() {
        return Err(invalid("skills.directory must be absolute"));
    }
    if !profile.read_roots.is_empty() && !profile.allow_external_read_roots {
        return Err(invalid(format!(
            "profile {} does not allow external read roots",
            profile.name
        )));
    }
    if !matches!(auth_mode, "global" | "account") {
        return Err(invalid("role auth mode must be 'global' or 'account'"));
    }
    if (auth_mode == "account") != auth_reference.is_some() {
        return Err(invalid(
            "account auth requires exactly one non-secret reference",
        ));
    }

    let mut skills = Vec::with_capacity(profile.skills.len());
    for name in &profile.skills {
        if !config_id(name) {
            return Err(invalid(format!("invalid canonical skill id: {name}")));
        }
        let source = skills_root.join(name);
        if !source.starts_with(skills_root) {
            return Err(invalid(format!("skill escapes canonical catalog: {name}")));
        }
        if !source.join("SKILL.md").is_file() {
            return Err(invalid(format!("canonical skill is not available: {name}")));
        }
        skills.push(ResolvedSkill {
            id: name.clone(),
            revision: tree_revision(&source)?,
        });
    }

    let mut servers = Vec::with_capacity(profile.mcp.len());
    for name in &profile.mcp {
        if !config_id(name) {
            return Err(invalid(format!("invalid role MCP id: {name}")));
        }
        let definition = mcp_catalog
            .get(name)
            .ok_or_else(|| invalid(format!("role references unknown MCP server: {name}")))?;
        servers.push(ResolvedMcp {
            id: name.clone(),
            transport: definition.transport.clone(),
            command: definition.command.to_string_lossy().into_owned(),
            args: definition.args.clone(),
            env_from: definition.env_from.clone(),
            approval_mode: definition.approval_mode.clone(),
        });
    }

    let mut plan = ResolvedRolePlan {
        role_name: profile.name.clone(),
        role_revision: profile.revision.clone(),
        prompt: profile.body.clone(),
        write: profile.write,
        network: profile.network,
        allow_external_read_roots: profile.allow_external_read_roots,
        read_roots: profiles::normalize_roots(&profile.read_roots),
        skills,
        mcp: servers,
        required_constraints: profile.required_constraints.clone(),
        auth_mode: auth_mode.to_string(),
        auth_reference: auth_reference.map(str::to_string),
        config_revision: String::new(),
    };
    plan.config_revision = canonical::sha256_hex(&canonical_payload(&plan), true);
    Ok(plan)
}

fn config_id(value: &str) -> bool {
    crate::config::name(value)
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn unique_len<'a>(items: impl Iterator<Item = &'a str>) -> usize {
    items.collect::<BTreeSet<_>>().len()
}

fn exact_object<'a>(value: &'a Value, keys: &[&str], path: &str) -> Result<&'a Map<String, Value>> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid(format!("{path} has an invalid shape")))?;
    let expected: BTreeSet<&str> = keys.iter().copied().collect();
    let actual: BTreeSet<&str> = object.keys().map(String::as_str).collect();
    if expected != actual {
        return Err(invalid(format!("{path} has an invalid shape")));
    }
    Ok(object)
}

fn boolean(value: &Value, path: &str) -> Result<bool> {
    value
        .as_bool()
        .ok_or_else(|| invalid(format!("{path} must be a boolean")))
}

fn text<'a>(value: &'a Value, path: &str, blank: bool) -> Result<&'a str> {
    let text = value
        .as_str()
        .ok_or_else(|| invalid(format!("{path} must be a string")))?;
    if text.contains('\0') || (!blank && text.trim().is_empty()) {
        let kind = if blank { "string" } else { "nonblank string" };
        return Err(invalid(format!("{path} must be a {kind}")));
    }
    Ok(text)
}

fn strings(value: &Value, path: &str, unique: bool) -> Result<Vec<String>> {
    let items = value
        .as_array()
        .ok_or_else(|| invalid(format!("{path} must be a list of strings")))?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(text(item, &format!("{path}[]"), true)?.to_string());
    }
    if unique && unique_len(out.iter().map(String::as_str)) != out.len() {
        return Err(invalid(format!("{path} must not contain duplicates")));
    }
    Ok(out)
}

/// Resolve existing absolute directories, requiring the caller's list to
/// already be the deduplicated minimal antichain it collapses to.
fn normalize_and_validate_roots(values: &[String]) -> Result<Vec<PathBuf>> {
    const INVALID: &str = "resolved role grants.read_roots must be a normalized antichain";
    let mut resolved = Vec::with_capacity(values.len());
    for value in values {
        let path = Path::new(value);
        if !path.is_absolute() {
            return Err(invalid(INVALID));
        }
        let real = path.canonicalize().map_err(|_| invalid(INVALID))?;
        if !real.is_dir() {
            return Err(invalid(INVALID));
        }
        resolved.push(real);
    }
    let normalized = profiles::normalize_roots(&resolved);
    let as_strings: Vec<String> = normalized
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect();
    if as_strings != values {
        return Err(invalid(
            "resolved role read_roots must be a normalized antichain",
        ));
    }
    Ok(normalized)
}

/// Purely lexical `..`/`.` collapse requiring an absolute input. Unlike
/// Python's `Path.resolve()`, this never touches the filesystem or follows
/// symlinks in existing prefixes: it only proves that a stored MCP `command`
/// string is *already* written in canonical form, which is what
/// `from_payload` needs and does not depend on whatever happens to exist on
/// the machine that later loads a frozen plan.
// ponytail: no symlink resolution; upgrade to a realpath-based check if a
// role plan ever needs to detect a command whose canonical form depends on
// resolving a symlink in an existing directory prefix.
fn lexical_absolute(input: &str) -> Result<String> {
    let path = Path::new(input);
    if !path.is_absolute() {
        return Err(invalid("resolved role mcp command must be absolute"));
    }
    let mut stack: Vec<Component> = Vec::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(stack.last(), Some(Component::Normal(_))) {
                    stack.pop();
                }
            }
            other => stack.push(other),
        }
    }
    let mut out = PathBuf::new();
    for component in stack {
        out.push(component.as_os_str());
    }
    Ok(out.to_string_lossy().into_owned())
}

/// Return the role-skill content revision used by canonical role plans.
///
/// Ports `src/agent_run/adapters/snapshot_tree.py:tree_revision`'s content
/// model: sorted `[path, "directory"]` / `[path, "file", bytes, sha256]`
/// entries hashed as compact canonical JSON. File mode is deliberately
/// excluded, matching Python.
// ponytail: plain `std::fs` traversal with `symlink_metadata` type checks,
// not Python's O_NOFOLLOW-descriptor walk. Adequate for hashing a trusted,
// local skills catalog; revisit with descriptor-based no-follow traversal if
// skills catalogs ever admit untrusted or concurrently-mutated content.
fn tree_revision(source: &Path) -> Result<String> {
    let mut entries: Vec<(String, Value)> = Vec::new();
    walk_tree(source, Path::new(""), &mut entries)?;
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let array = Value::Array(entries.into_iter().map(|(_, value)| value).collect());
    Ok(canonical::sha256_hex(&array, true))
}

fn walk_tree(dir: &Path, prefix: &Path, out: &mut Vec<(String, Value)>) -> Result<()> {
    let mut names: Vec<_> = std::fs::read_dir(dir)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    names.sort();
    for name in names {
        let full = dir.join(&name);
        let relative = prefix.join(&name);
        let portable = relative.to_string_lossy().into_owned();
        let metadata = std::fs::symlink_metadata(&full)?;
        if metadata.is_dir() {
            out.push((portable.clone(), json!([portable, "directory"])));
            walk_tree(&full, &relative, out)?;
        } else if metadata.is_file() {
            let bytes = std::fs::read(&full)?;
            let sha = agent_run_platform::fs::sha256(&bytes);
            out.push((
                portable.clone(),
                json!([portable, "file", bytes.len(), sha]),
            ));
        } else {
            return Err(invalid(format!(
                "snapshot entry must be regular: {portable}"
            )));
        }
    }
    Ok(())
}
