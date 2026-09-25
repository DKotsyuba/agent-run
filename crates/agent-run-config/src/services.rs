//! Broker-owned foreground service declarations; no provider-specific daemon logic.

use agent_run_domain::{error::invalid, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf};

/// One process kept warm by the broker, independently of individual harness trees.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedService {
    /// Absolute executable that stays in the foreground; self-daemonizing commands are unsupported.
    pub command: PathBuf,
    /// Literal arguments; never interpolated by agent-run.
    #[serde(default)]
    pub args: Vec<String>,
    /// Absolute working directory for both the service and its readiness probe.
    pub cwd: PathBuf,
    /// Explicit host environment names; values are resolved at launch and never stored in service records.
    #[serde(default)]
    pub env_from: Vec<String>,
    /// Command whose zero exit confirms application readiness, separately from PID ownership.
    pub readiness: ReadinessProbe,
    /// Check for an existing external service before spawning; its process is never owned or stopped.
    /// Opt-in probes must support external mode without AGENT_RUN_SERVICE_PID.
    #[serde(default, skip_serializing_if = "is_false")]
    pub reuse_existing: bool,
    /// Maximum cold-start readiness wait in seconds, between 1 and 300.
    #[serde(default = "startup_timeout")]
    pub startup_timeout_seconds: u64,
    /// Seconds without active or unresolved agents, counted after the last lease is released.
    #[serde(default = "idle_timeout")]
    pub idle_timeout_seconds: u64,
    /// Maximum interval in seconds between health probes while this service is warm.
    #[serde(default = "monitor_interval")]
    pub monitor_interval_seconds: u64,
    /// Grace between TERM and KILL, in seconds; accepts 0 through 30.
    #[serde(default = "stop_grace")]
    pub stop_grace_seconds: u64,
}

/// Bounded health command; output is discarded so secrets cannot enter service diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadinessProbe {
    /// Absolute executable which checks this service's application endpoint.
    pub command: PathBuf,
    /// Literal arguments; identity is supplied separately through AGENT_RUN_SERVICE_* variables.
    #[serde(default)]
    pub args: Vec<String>,
    /// Deadline for one probe, in seconds, between 1 and 30.
    #[serde(default = "probe_timeout")]
    pub timeout_seconds: u64,
}

/// Omits the disabled opt-in so historical frozen service revisions remain unchanged.
fn is_false(value: &bool) -> bool {
    !value
}

/// Default cold-start budget in seconds.
fn startup_timeout() -> u64 {
    60
}
/// Owner-selected default inactivity period in seconds: thirty minutes after the last active agent.
fn idle_timeout() -> u64 {
    1800
}
/// Default health observation interval in seconds.
fn monitor_interval() -> u64 {
    5
}
/// Default termination grace in seconds.
fn stop_grace() -> u64 {
    2
}
/// Default per-probe deadline in seconds.
fn probe_timeout() -> u64 {
    2
}

/// Checks a stable, bounded service id suitable for configuration and durable keys.
pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

impl ManagedService {
    /// Rejects ambiguous paths, unbounded timing, malformed argv and reserved environment names.
    /// Reads no credentials and does not execute the declared command or probe.
    pub fn validate(&self) -> Result<()> {
        for path in [&self.command, &self.cwd, &self.readiness.command] {
            if !path.is_absolute() || path.to_str().is_none_or(|p| p.contains('\0')) {
                return Err(invalid(
                    "service executable, cwd and probe paths must be absolute UTF-8 paths",
                ));
            }
        }
        if self.args.len() > 128
            || self.readiness.args.len() > 128
            || self
                .args
                .iter()
                .chain(&self.readiness.args)
                .any(|arg| arg.len() > 8192 || arg.contains('\0'))
            || !(1..=300).contains(&self.startup_timeout_seconds)
            || !(1..=86400).contains(&self.idle_timeout_seconds)
            || !(1..=60).contains(&self.monitor_interval_seconds)
            || !(1..=30).contains(&self.readiness.timeout_seconds)
            || self.stop_grace_seconds > 30
        {
            return Err(invalid("invalid service arguments or lifecycle bounds"));
        }
        let mut names = BTreeSet::new();
        for name in &self.env_from {
            if name.is_empty()
                || name.len() > 128
                || name.starts_with("AGENT_RUN_")
                || name.as_bytes()[0].is_ascii_digit()
                || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                || !names.insert(name)
            {
                return Err(invalid("invalid or duplicate service environment name"));
            }
        }
        Ok(())
    }

    /// Returns the exact configuration revision without including resolved host environment values.
    pub fn revision(&self) -> Result<String> {
        self.validate()?;
        Ok(agent_run_domain::canonical::sha256_hex(
            &serde_json::to_value(self)?,
            true,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Disabled reuse preserves historical frozen revisions; enabling it is an explicit revision change.
    #[test]
    fn external_reuse_is_opt_in_without_changing_legacy_serialization() {
        let legacy = serde_json::json!({"command":"/bin/sleep","args":["20"],"cwd":"/tmp",
            "env_from":[],"readiness":{"command":"/usr/bin/true","args":[],"timeout_seconds":2},
            "startup_timeout_seconds":60,"idle_timeout_seconds":1800,"monitor_interval_seconds":5,
            "stop_grace_seconds":2});
        let mut service: ManagedService = serde_json::from_value(legacy.clone()).unwrap();
        assert!(!service.reuse_existing);
        assert_eq!(serde_json::to_value(&service).unwrap(), legacy);
        let original = service.revision().unwrap();
        service.reuse_existing = true;
        assert_ne!(service.revision().unwrap(), original);
    }
}
