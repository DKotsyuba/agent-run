//! Never upgrade tool filtering or home isolation into an OS sandbox guarantee.
//!
//! Ports `src/agent_run/effective_policy.py`: `DeclaredCapability`,
//! `Enforcement`, `_satisfies`, `effective_policy()`, and
//! `admission_decision()`. `evaluate()`/`EffectivePolicy::admit()` are the
//! pre-existing narrow entry point used outside this crate and are now
//! implemented in terms of the ported functions below rather than duplicated.
use crate::{config::Runtime, profiles::Profile};
pub use agent_run_domain::domain::Constraint;
use agent_run_domain::{error::invalid, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Enforcement {
    ToolFilter,
    RuntimeEnforced,
    OsEnforced,
    Advisory,
    Unsupported,
}

/// One recognized constraint's stable wire name (matches Python's
/// `Constraint.value`, via the same `#[serde(rename_all = "snake_case")]`
/// mapping already declared on `Constraint`).
pub fn constraint_name(constraint: Constraint) -> String {
    serde_json::to_value(constraint)
        .expect("Constraint always serializes")
        .as_str()
        .expect("Constraint serializes as a string")
        .to_string()
}

/// The inverse of [`constraint_name`]; `None` for any unrecognized name.
pub fn constraint_from_name(name: &str) -> Option<Constraint> {
    serde_json::from_value(serde_json::Value::String(name.to_string())).ok()
}

/// A runtime's enforcement claim for one constraint (ported from Python
/// `effective_policy.DeclaredCapability`). `scope` states the exact boundary
/// covered, `reason` names the backend evidence, and an empty `platforms`
/// means the claim applies everywhere. The provider must not upgrade this
/// declaration from configuration alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclaredCapability {
    pub enforcement: Enforcement,
    pub scope: String,
    pub reason: String,
    #[serde(default)]
    pub platforms: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evidence {
    pub constraint: Constraint,
    pub enforcement: Enforcement,
    pub supported: bool,
    pub required: bool,
    pub scope: String,
    pub platform: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectivePolicy {
    pub runtime_name: String,
    pub platform: String,
    pub constraints: Vec<Evidence>,
}

/// Typed admission result listing only required unsupported constraints.
/// `allowed` is true exactly when `unsupported_required` is empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionDecision {
    pub allowed: bool,
    pub unsupported_required: Vec<Constraint>,
}

/// Port of Python `_satisfies`: only a runtime- or OS-enforced layer
/// satisfies an isolation constraint; tool filtering satisfies only the
/// removal of built-in web tools, and advisory/unsupported evidence never
/// satisfies anything.
fn satisfies(constraint: Constraint, enforcement: Enforcement) -> bool {
    match enforcement {
        Enforcement::Advisory | Enforcement::Unsupported => false,
        Enforcement::ToolFilter => constraint == Constraint::WebToolsDisabled,
        Enforcement::RuntimeEnforced | Enforcement::OsEnforced => true,
    }
}

pub fn current_platform() -> &'static str {
    if cfg!(target_os = "macos") {
        "darwin"
    } else {
        "linux"
    }
}

// Conservative reasons used until a backend supplies narrower verified evidence.
fn unsupported_reason(constraint: Constraint) -> &'static str {
    match constraint {
        Constraint::WebToolsDisabled => "the effective profile authorizes built-in web tools",
        Constraint::ExternalNetworkIsolation => {
            "tool filtering does not prevent shell or child-process external sockets"
        }
        Constraint::LoopbackTcpIsolation => {
            "no backend declared isolation from loopback or host-local TCP listeners"
        }
        Constraint::UnixIpcIsolation => "no backend declared Unix-domain socket isolation",
        Constraint::McpIpcIsolation => {
            "no backend declared isolation from configured MCP transports"
        }
        Constraint::FilesystemWriteIsolation => {
            "profile write grants and runtime HOME do not provide OS write containment"
        }
        Constraint::FilesystemReadIsolation => {
            "profile read roots and runtime HOME do not provide OS read containment"
        }
        Constraint::PluginImmutability => {
            "the runtime did not declare validated materialization of all selected plugin assets"
        }
    }
}

fn unsupported(constraint: Constraint, platform: &str, reason: String, required: bool) -> Evidence {
    Evidence {
        constraint,
        enforcement: Enforcement::Unsupported,
        supported: false,
        required,
        scope: constraint_name(constraint),
        platform: platform.into(),
        reason,
    }
}

/// Port of Python `effective_policy()`: resolve effective-policy evidence for
/// every known constraint, in stable enum order, without changing runtime
/// permissions. `required` is explicit; profile grants never imply it.
pub fn effective_policy(
    profile: &Profile,
    runtime_name: &str,
    platform: &str,
    capabilities: &BTreeMap<Constraint, DeclaredCapability>,
    required: &BTreeSet<Constraint>,
) -> EffectivePolicy {
    let constraints = Constraint::ALL
        .into_iter()
        .map(|constraint| {
            let is_required = required.contains(&constraint);
            if let Some(capability) = capabilities.get(&constraint) {
                if !capability.platforms.is_empty() && !capability.platforms.contains(platform) {
                    let supported: Vec<&str> =
                        capability.platforms.iter().map(String::as_str).collect();
                    return unsupported(
                        constraint,
                        platform,
                        format!("declared only for platforms: {}", supported.join(", ")),
                        is_required,
                    );
                }
                return Evidence {
                    constraint,
                    enforcement: capability.enforcement,
                    supported: satisfies(constraint, capability.enforcement),
                    required: is_required,
                    scope: capability.scope.clone(),
                    platform: platform.into(),
                    reason: capability.reason.clone(),
                };
            }
            if constraint == Constraint::WebToolsDisabled {
                return if !profile.network {
                    Evidence {
                        constraint,
                        enforcement: Enforcement::ToolFilter,
                        supported: true,
                        required: is_required,
                        scope: "runtime WebFetch and WebSearch tools only".into(),
                        platform: platform.into(),
                        reason: "the effective profile removes built-in web fetch and search tools"
                            .into(),
                    }
                } else {
                    unsupported(
                        constraint,
                        platform,
                        unsupported_reason(constraint).into(),
                        is_required,
                    )
                };
            }
            unsupported(
                constraint,
                platform,
                unsupported_reason(constraint).into(),
                is_required,
            )
        })
        .collect();
    EffectivePolicy {
        runtime_name: runtime_name.into(),
        platform: platform.into(),
        constraints,
    }
}

/// Port of Python `admission_decision()`: allow unless an explicit
/// requirement lacks sufficient enforcement.
pub fn admission_decision(policy: &EffectivePolicy) -> AdmissionDecision {
    let unsupported_required: Vec<Constraint> = policy
        .constraints
        .iter()
        .filter(|item| item.required && !item.supported)
        .map(|item| item.constraint)
        .collect();
    AdmissionDecision {
        allowed: unsupported_required.is_empty(),
        unsupported_required,
    }
}

/// Legacy narrow entry point: derives capabilities from `runtime`/`profile`
/// the same way the pre-port implementation did, then delegates to
/// [`effective_policy`]. `runtime.required_constraints` is not read here;
/// callers pass the already-tightened `profile.required_constraints`
/// (the effective requirement is the union of role and request; see
/// `docs/api.md`).
pub fn evaluate(runtime_name: &str, runtime: &Runtime, profile: &Profile) -> EffectivePolicy {
    let platform = current_platform();
    let mut capabilities = BTreeMap::new();
    if runtime.plugins.is_empty() {
        capabilities.insert(
            Constraint::PluginImmutability,
            DeclaredCapability {
                enforcement: Enforcement::RuntimeEnforced,
                scope: constraint_name(Constraint::PluginImmutability),
                reason: "no configured plugin assets".into(),
                platforms: BTreeSet::new(),
            },
        );
    }
    effective_policy(
        profile,
        runtime_name,
        platform,
        &capabilities,
        &profile.required_constraints,
    )
}

impl EffectivePolicy {
    pub fn admit(&self) -> Result<()> {
        let decision = admission_decision(self);
        if !decision.allowed {
            let names: Vec<String> = decision
                .unsupported_required
                .iter()
                .map(|constraint| constraint_name(*constraint))
                .collect();
            return Err(invalid(format!(
                "required policy constraints are not enforced: {}",
                names.join(", ")
            )));
        }
        Ok(())
    }
}
