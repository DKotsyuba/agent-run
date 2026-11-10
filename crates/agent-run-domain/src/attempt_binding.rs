//! Private, disabled-by-default attempt binding v1 data; these values do not
//! authenticate a caller or constitute proof without the configured transports.
use crate::{Result, StoreId, domain::AgentId, error::invalid};
use serde::{Deserialize, Serialize};

fn id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}
fn hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|u| u.to_string() == value)
}

/// Pinned component route, resolved from authenticated configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub component_store_id: String,
    pub route_id: String,
    pub worker_uid: u32,
}
impl Route {
    pub fn validate(&self) -> Result<()> {
        if !id(&self.component_store_id) || !id(&self.route_id) {
            return Err(invalid("invalid attempt route"));
        }
        Ok(())
    }
}

/// Durable Run attempt identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptRef {
    pub run_store_id: StoreId,
    pub lineage_root_id: AgentId,
    pub execution_id: AgentId,
    pub sequence: u32,
    pub attempt_id: String,
    pub launch_id: String,
}
impl AttemptRef {
    pub fn validate(&self) -> Result<()> {
        if self.sequence == 0 || !id(&self.attempt_id) || !uuid(&self.launch_id) {
            return Err(invalid("invalid attempt identity"));
        }
        Ok(())
    }
}

/// Exact kernel incarnation and pinned host label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Incarnation {
    pub host_id: String,
    pub boot_id: String,
    pub pid_namespace_inode: u64,
    pub uid: u32,
    pub pid: u32,
    pub start_ticks: u64,
}

/// Supervisor-only immutable proof after managed native role verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleQualification {
    pub launch_id: String,
    pub attempt_id: String,
    pub harness: String,
    pub role_digest: String,
    pub config_digest: String,
    pub inventory_digest: String,
    pub incarnation: Incarnation,
}
impl RoleQualification {
    pub fn validate(&self) -> Result<()> {
        self.incarnation.validate()?;
        if !uuid(&self.launch_id)
            || !id(&self.attempt_id)
            || !matches!(self.harness.as_str(), "codex" | "claude-code")
            || !hash(&self.role_digest)
            || !hash(&self.config_digest)
            || !hash(&self.inventory_digest)
        {
            return Err(invalid("invalid role qualification"));
        }
        Ok(())
    }
}
impl Incarnation {
    pub fn validate(&self) -> Result<()> {
        if !id(&self.host_id)
            || !uuid(&self.boot_id)
            || self.pid_namespace_inode == 0
            || self.pid <= 1
            || self.start_ticks == 0
        {
            return Err(invalid("invalid incarnation"));
        }
        Ok(())
    }
}

/// Bridge's original root actor, independently checked against native session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Actor {
    pub address: String,
    pub role: String,
    pub native_id: String,
    pub epoch: u64,
}
impl Actor {
    pub fn validate(&self) -> Result<()> {
        if !id(&self.address) || self.role != "root" || !id(&self.native_id) || self.epoch == 0 {
            return Err(invalid("invalid attempt actor"));
        }
        Ok(())
    }
}

/// Pool's generation-bound immutable member association.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub pool_store_id: String,
    pub pool_id: String,
    pub member_id: String,
    pub binding_generation: u64,
    pub attempt: AttemptRef,
    pub actor: Actor,
    pub run_route: Route,
    pub bridge_route: Route,
}
impl Binding {
    pub fn validate(&self) -> Result<()> {
        self.attempt.validate()?;
        self.actor.validate()?;
        self.run_route.validate()?;
        self.bridge_route.validate()?;
        if !id(&self.pool_store_id)
            || !id(&self.pool_id)
            || !id(&self.member_id)
            || self.binding_generation == 0
            || self.run_route.component_store_id != self.attempt.run_store_id.to_string()
            || self.run_route.worker_uid != self.bridge_route.worker_uid
        {
            return Err(invalid("invalid attempt binding"));
        }
        Ok(())
    }
}

/// Immutable key for authenticated lookup of Bridge's consumed original hook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumedRef {
    pub bridge_route: Route,
    pub producer: String,
    pub consume_request_id: String,
    pub invocation_key: String,
    pub security_digest: String,
    pub issuer_epoch: String,
}

/// Original protected hook observation returned by an authenticated Bridge reader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub incarnation: Incarnation,
    pub launch_id: String,
    pub actor: Actor,
    pub native_call_id: String,
    pub tool_name: String,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
}
impl Observation {
    pub fn validate(&self) -> Result<()> {
        self.incarnation.validate()?;
        self.actor.validate()?;
        if !uuid(&self.launch_id)
            || !id(&self.native_call_id)
            || !id(&self.tool_name)
            || self.expires_at_ms < self.observed_at_ms
        {
            return Err(invalid("invalid original observation"));
        }
        Ok(())
    }
}

/// Bridge's immutable consumed association, accepted only after authenticated lookup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumeReceipt {
    pub producer: String,
    pub consume_request_id: String,
    pub invocation_key: String,
    pub security_digest: String,
    pub observation: Observation,
    pub issuer_epoch: String,
    pub consumed_at_ms: u64,
}
impl ConsumedRef {
    pub fn validate(&self) -> Result<()> {
        self.bridge_route.validate()?;
        if !id(&self.producer)
            || !id(&self.consume_request_id)
            || !id(&self.invocation_key)
            || !hash(&self.security_digest)
            || !id(&self.issuer_epoch)
            || !uuid(&self.bridge_route.component_store_id)
        {
            return Err(invalid("invalid consumed reference"));
        }
        Ok(())
    }
}

/// Persisted one-step execution lineage proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineageStep {
    pub execution_id: AgentId,
    pub parent_execution_id: Option<AgentId>,
    pub sequence: u32,
}

/// Proved segment after checking the entire root-to-tip chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuccessorProof {
    pub lineage_root_id: AgentId,
    pub bound_execution_id: AgentId,
    pub current_execution_id: AgentId,
    pub chain: Vec<LineageStep>,
}

/// Durable currentness observation, never a future liveness guarantee.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CurrentReceipt {
    pub receipt_id: String,
    pub purpose: String,
    pub lineage_root_id: AgentId,
    pub bound_execution_id: AgentId,
    pub attempt: AttemptRef,
    pub incarnation: Incarnation,
    pub actor: Actor,
    pub successor_proof: SuccessorProof,
}

/// An already-authorized ingress request; route facts come from configuration.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngressCurrent {
    pub expected_store_id: StoreId,
    pub request_id: String,
    pub bridge_route: Route,
    pub pool_route: Route,
    pub run_route: Route,
    pub producer: String,
    pub bridge_context_ref: String,
    pub invocation_key: String,
}

/// Pool's egress rebind query; the candidate actor comes only from Bridge.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EgressCurrent {
    pub expected_store_id: StoreId,
    pub request_id: String,
    pub lineage_root_id: AgentId,
    pub bound_execution_id: AgentId,
    pub bridge_route: Route,
    pub pool_route: Route,
    pub run_route: Route,
    pub producer: String,
    pub host_id: String,
}
impl EgressCurrent {
    pub fn validate(&self) -> Result<()> {
        self.bridge_route.validate()?;
        self.pool_route.validate()?;
        self.run_route.validate()?;
        if !id(&self.request_id)
            || !id(&self.producer)
            || !id(&self.host_id)
            || !uuid(&self.bridge_route.component_store_id)
            || self.run_route.component_store_id != self.expected_store_id.to_string()
            || self.run_route.worker_uid != self.bridge_route.worker_uid
            || self.run_route.worker_uid != self.pool_route.worker_uid
        {
            return Err(invalid("invalid egress current request"));
        }
        Ok(())
    }
}
impl std::fmt::Debug for IngressCurrent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("IngressCurrent([REDACTED])")
    }
}
impl IngressCurrent {
    pub fn validate(&self) -> Result<()> {
        self.bridge_route.validate()?;
        self.pool_route.validate()?;
        self.run_route.validate()?;
        if !id(&self.request_id)
            || !id(&self.producer)
            || !id(&self.bridge_context_ref)
            || !id(&self.invocation_key)
            || self.run_route.component_store_id != self.expected_store_id.to_string()
            || self.run_route.worker_uid != self.bridge_route.worker_uid
            || self.run_route.worker_uid != self.pool_route.worker_uid
            || !uuid(&self.bridge_route.component_store_id)
        {
            return Err(invalid("invalid current request"));
        }
        Ok(())
    }
}

/// Exact admitted snapshot; no guard and no additional authority after replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Admission {
    pub admission_id: String,
    pub intent_id: String,
    pub security_digest: String,
    pub policy: String,
    pub binding: Binding,
    pub incarnation: Incarnation,
    pub successor_proof: SuccessorProof,
    pub consumed_ref: ConsumedRef,
}

/// Typed internal preparation after Bridge and caller authentication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionRequest {
    pub version: u32,
    pub expected_store_id: StoreId,
    pub request_id: String,
    pub intent_id: String,
    pub security_digest: String,
    pub binding: Binding,
    pub consumed_ref: ConsumedRef,
    pub policy: String,
    pub pool_route: Route,
}
impl AdmissionRequest {
    pub fn validate(&self) -> Result<()> {
        self.binding.validate()?;
        self.consumed_ref.validate()?;
        self.pool_route.validate()?;
        if self.version != 1
            || !id(&self.intent_id)
            || self.request_id != self.intent_id
            || !hash(&self.security_digest)
            || self.security_digest != self.consumed_ref.security_digest
            || self.policy != "admission_snapshot"
            || self.expected_store_id != self.binding.attempt.run_store_id
            || self.binding.bridge_route != self.consumed_ref.bridge_route
            || self.pool_route.component_store_id != self.binding.pool_store_id
            || self.pool_route.worker_uid != self.binding.run_route.worker_uid
        {
            return Err(invalid("invalid attempt admission"));
        }
        Ok(())
    }
}

/// Validates a bounded opaque protocol ID without returning its value in errors.
pub fn validate_id(value: &str) -> Result<()> {
    if id(value) {
        Ok(())
    } else {
        Err(invalid("invalid attempt operation id"))
    }
}
/// Validates one exact SHA-256 spelling.
pub fn validate_digest(value: &str) -> Result<()> {
    if hash(value) {
        Ok(())
    } else {
        Err(invalid("invalid attempt digest"))
    }
}

/// Require the canonical UUID spelling used by Bridge stores and launches.
pub fn validate_uuid(value: &str) -> Result<()> {
    if uuid(value) {
        Ok(())
    } else {
        Err(invalid("invalid attempt UUID"))
    }
}

/// Section 6 recursively tagged canonical tree. It preserves integer versus
/// binary64 categories and normalizes negative zero without rewriting strings.
fn tagged(value: &serde_json::Value) -> Result<serde_json::Value> {
    use serde_json::{Value, json};
    Ok(match value {
        Value::Null => json!(["n"]),
        Value::Bool(v) => json!(["b", v]),
        Value::String(v) => json!(["s", v]),
        Value::Number(n) if n.is_i64() => json!([
            "i",
            n.as_i64()
                .ok_or_else(|| invalid("invalid canonical number"))?
                .to_string()
        ]),
        Value::Number(n) if n.is_u64() => json!([
            "i",
            n.as_u64()
                .ok_or_else(|| invalid("invalid canonical number"))?
                .to_string()
        ]),
        Value::Number(n) => {
            let v = n
                .as_f64()
                .filter(|v| v.is_finite())
                .ok_or_else(|| invalid("invalid canonical number"))?;
            let bits = if v == 0.0 { 0 } else { v.to_bits() };
            json!(["f", format!("{bits:016x}")])
        }
        Value::Array(items) => json!(["a", items.iter().map(tagged).collect::<Result<Vec<_>>>()?]),
        Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort();
            let pairs = keys
                .into_iter()
                .map(|key| Ok(json!([key, tagged(&map[key])?])))
                .collect::<Result<Vec<_>>>()?;
            json!(["o", pairs])
        }
    })
}
fn tagged_digest(domain: &[u8], value: &serde_json::Value) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(crate::canonical::dumps(&tagged(value)?, false));
    Ok(crate::canonical::hex_digest(&hash.finalize()))
}

/// P3 fingerprint of the exact normalized private enrollment parameter tree.
pub fn enrollment_start_fingerprint(value: &serde_json::Value) -> Result<String> {
    tagged_digest(b"agent-mcp/enrollment/start/v1\0", value)
}

/// Immutable admission fingerprint under the frozen admission domain.
pub fn admission_fingerprint(request: &AdmissionRequest) -> Result<String> {
    tagged_digest(
        b"agent-mcp/attempt-binding/admission/v1\0",
        &serde_json::to_value(request)?,
    )
}
/// Private exact-replay fingerprint for an ingress currentness request.
pub fn current_fingerprint(request: &IngressCurrent) -> Result<String> {
    tagged_digest(
        b"agent-mcp/attempt-binding/current/v1\0",
        &serde_json::to_value(request)?,
    )
}
/// Private exact-replay fingerprint, distinct from ingress currentness.
pub fn egress_current_fingerprint(request: &EgressCurrent) -> Result<String> {
    tagged_digest(
        b"agent-mcp/attempt-binding/current-egress/v1\0",
        &serde_json::to_value(request)?,
    )
}

/// Frozen v1 terminal Pool outcome fingerprint under its distinct domain.
pub fn settlement_digest(
    pool_store_id: &str,
    intent_id: &str,
    security_digest: &str,
    state: &str,
) -> Result<String> {
    validate_id(pool_store_id)?;
    validate_id(intent_id)?;
    validate_digest(security_digest)?;
    if !matches!(state, "committed" | "aborted") {
        return Err(invalid("invalid settlement state"));
    }
    tagged_digest(
        b"agent-mcp/attempt-binding/settlement/v1\0",
        &serde_json::json!({"version":1,"pool_store_id":pool_store_id,"intent_id":intent_id,"security_digest":security_digest,"state":state}),
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn settlement_shared_vector() {
        assert_eq!(
            super::settlement_digest("pool-store-uuid", "intent-1", &"a".repeat(64), "committed")
                .unwrap(),
            "10a5159c5c085fd018534a640862ed8600fdf334cbfac41be9964659d72de8eb"
        );
        assert_ne!(
            super::settlement_digest("pool-store-uuid", "intent-1", &"a".repeat(64), "committed")
                .unwrap(),
            super::settlement_digest("pool-store-uuid", "intent-1", &"a".repeat(64), "aborted")
                .unwrap()
        );
    }
}
