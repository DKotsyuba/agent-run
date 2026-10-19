//! Strict provider start input, distinct from readable historical runtime requests.

use crate::{
    Result,
    catalog::ProviderId,
    domain::{Constraint, OrchestratorRef, StartRequest},
    types::AccountLabel,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

/// A new orchestrator-selected provider/model request.
///
/// Unknown fields, including the historical `runtime` selector, fail
/// deserialization. Account names are provider-local labels; absence means
/// quota-side automatic selection. Profile grants are resolved and frozen by
/// the service, and candidates are supplied separately by a trusted producer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderStartRequest {
    /// Explicit configured provider identity.
    pub provider: ProviderId,
    /// Explicit offered model id; the service never chooses a model.
    pub model: String,
    /// Canonical role profile name.
    pub profile: String,
    /// User task text, bounded like historical starts.
    pub task: String,
    /// Existing absolute work directory.
    pub workdir: PathBuf,
    /// Caller write intent; a canonical role remains authoritative.
    #[serde(default)]
    pub write: bool,
    /// Existing absolute read roots requested in addition to the workdir.
    #[serde(default)]
    pub read_roots: Vec<PathBuf>,
    /// Optional provider-local pinned account label.
    #[serde(default)]
    pub account: Option<AccountLabel>,
    /// Optional model effort chosen by the orchestrator.
    #[serde(default)]
    pub effort: Option<String>,
    /// Optional human display label, validated like the historical request's
    /// field and inherited by explicit resumes that omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Existing Codex fast-mode request flag.
    #[serde(default)]
    pub fast: bool,
    /// New admissions default to callback completion. Explicit false selects
    /// legacy compatibility. Always serialize the choice so transport decoding
    /// cannot replace an explicit false with the new-admission default.
    #[serde(default = "crate::worker::default_explicit_finish")]
    pub explicit_finish: bool,
    /// Frozen health policy; recover explicitly authorizes bounded model probes.
    #[serde(
        default = "crate::health::observe",
        skip_serializing_if = "crate::health::PingPolicy::is_observe"
    )]
    pub idle_ping: crate::health::PingPolicy,
    /// Existing output schema for supporting harnesses.
    #[serde(default)]
    pub output_schema: Option<serde_json::Map<String, serde_json::Value>>,
    /// Scoped idempotency key.
    #[serde(default)]
    pub request_id: Option<String>,
    /// Optional external orchestrator namespace.
    #[serde(default)]
    pub orchestrator: Option<OrchestratorRef>,
    /// Constraints requested in addition to those required by the role/model.
    #[serde(default, deserialize_with = "unique_constraints")]
    pub required_constraints: BTreeSet<Constraint>,
    /// Non-grant assertion on the resolved MCP selection: at most eight servers
    /// and 32 exact unprefixed tool names per server. Empty lists require only
    /// server presence. Empty/omitted maps serialize absent for historical replay.
    #[serde(
        default,
        deserialize_with = "required_mcp",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub required_mcp: BTreeMap<String, Vec<String>>,
}

/// Decodes bounded MCP assertions without losing duplicate object keys or tools.
/// Null and non-object input fail; names and canonical ordering are validated here
/// and again by request validation for callers constructing the public type directly.
fn required_mcp<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, Vec<String>>, D::Error> {
    /// Streams the bounded map, rejecting duplicate server keys before insertion.
    struct RequiredMcp;
    impl<'de> serde::de::Visitor<'de> for RequiredMcp {
        /// Canonical server map; each value is a sorted exact tool set.
        type Value = BTreeMap<String, Vec<String>>;
        /// Describes the sole accepted wire representation.
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a bounded map of MCP servers to exact tool names")
        }
        /// Retains exact names and refuses duplicates, wildcards and oversized maps.
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut result = BTreeMap::new();
            while let Some((server, tools)) = map.next_entry::<String, Vec<String>>()? {
                if result.len() >= 8 || result.insert(server, tools).is_some() {
                    return Err(serde::de::Error::custom(
                        "duplicate or excessive required MCP servers",
                    ));
                }
            }
            normalize_required_mcp(&mut result).map_err(serde::de::Error::custom)?;
            Ok(result)
        }
    }
    deserializer.deserialize_map(RequiredMcp)
}

/// Validates the config name/tool grammar and limits, sorting each exact tool set.
/// No server or tool is selected or granted by this normalization.
fn normalize_required_mcp(requirements: &mut BTreeMap<String, Vec<String>>) -> Result<()> {
    let name = |value: &str| {
        (1..=128).contains(&value.len())
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
    };
    if requirements.len() > 8 {
        return Err(crate::error::invalid("required_mcp exceeds eight servers"));
    }
    for (server, tools) in requirements {
        if !name(server)
            || !server.as_bytes()[0].is_ascii_alphanumeric()
            || tools.len() > 32
            || tools.iter().any(|tool| !name(tool))
        {
            return Err(crate::error::invalid(
                "required_mcp requires bounded exact server and tool names",
            ));
        }
        tools.sort();
        if tools.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(crate::error::invalid("duplicate required MCP tool"));
        }
    }
    Ok(())
}

/// Decodes required constraints without silently collapsing duplicate input.
fn unique_constraints<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeSet<Constraint>, D::Error> {
    let values = Vec::<Constraint>::deserialize(deserializer)?;
    let unique: BTreeSet<_> = values.iter().copied().collect();
    if unique.len() != values.len() {
        return Err(serde::de::Error::custom("duplicate provider constraints"));
    }
    Ok(unique)
}

impl ProviderStartRequest {
    /// Validates/canonicalizes MCP assertions, paths, task, effort, namespace, orchestrator transport
    /// and the optional human label; stores canonical paths, labels and transport
    /// in place. Invalid values return `ValidationError` before admission.
    /// Historical runtime selectors remain excluded from public v2 input.
    pub fn validate(&mut self) -> Result<()> {
        normalize_required_mcp(&mut self.required_mcp)?;
        let mut projection = self.storage_projection();
        projection.validate()?;
        self.workdir = projection.workdir;
        self.read_roots = projection.read_roots;
        self.display_name = projection.display_name;
        self.orchestrator = projection.orchestrator;
        Ok(())
    }

    /// Validates immutable fields and canonicalizes MCP assertions for exact replay
    /// without requiring live directories. Never selects or grants requested tools.
    /// A new request still calls `validate` before admission and canonicalizes paths.
    pub fn validate_intent(&mut self) -> Result<()> {
        normalize_required_mcp(&mut self.required_mcp)?;
        let mut projection = self.storage_projection();
        projection.validate_intent()?;
        self.display_name = projection.display_name;
        self.orchestrator = projection.orchestrator;
        Ok(())
    }

    /// Projects persisted provider intent without its obsolete run allowance.
    /// Callers verify the original raw replay digest before this projection;
    /// original JSON is never rewritten. Remaining fields decode strictly and
    /// malformed/unknown fields return typed JSON errors. Public input uses the
    /// ordinary strict decoder, which rejects the removed allowance parameter.
    pub fn from_history(mut document: serde_json::Value) -> Result<Self> {
        if let Some(object) = document.as_object_mut() {
            object.remove("timeout_seconds");
            object
                .entry("explicit_finish")
                .or_insert(serde_json::Value::Bool(false));
        }
        Ok(serde_json::from_value(document)?)
    }

    /// Produces the existing read-model projection for staged store readers.
    ///
    /// Its `runtime` field is only a compatibility projection; the versioned
    /// identity and embedded provider request, never this string, prove a v2
    /// admission. It is not a public runtime launch alias.
    pub fn storage_projection(&self) -> StartRequest {
        StartRequest {
            runtime: self.provider.as_str().into(),
            model: self.model.clone(),
            profile: self.profile.clone(),
            task: self.task.clone(),
            workdir: self.workdir.clone(),
            write: self.write,
            fast: self.fast,
            explicit_finish: self.explicit_finish,
            idle_ping: self.idle_ping,
            effort: self.effort.clone(),
            display_name: self.display_name.clone(),
            read_roots: self.read_roots.clone(),
            output_schema: self.output_schema.clone(),
            orchestrator: self.orchestrator.clone(),
            request_id: self.request_id.clone(),
            account: self.account.as_ref().map(|label| label.as_str().into()),
            required_constraints: self.required_constraints.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    /// Minimal intent without any filesystem dependency.
    fn request() -> Value {
        json!({"provider":"p","model":"m","profile":"review","task":"task","workdir":"/already-removed"})
    }

    /// Empty assertions preserve old fingerprints; maps and tool sets are canonical.
    #[test]
    fn required_mcp_is_canonical_and_absence_compatible() {
        let absent: ProviderStartRequest = serde_json::from_value(request()).unwrap();
        let mut value = request();
        value["required_mcp"] = json!({});
        let empty: ProviderStartRequest = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(absent).unwrap(),
            serde_json::to_value(empty).unwrap()
        );
        value["required_mcp"] = json!({"z":[], "a":["write","read"]});
        let first: ProviderStartRequest = serde_json::from_value(value.clone()).unwrap();
        value["required_mcp"] = json!({"a":["read","write"], "z":[]});
        let second: ProviderStartRequest = serde_json::from_value(value).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.required_mcp["a"], ["read", "write"]);
        assert_eq!(
            ProviderStartRequest::from_history(serde_json::to_value(&first).unwrap()).unwrap(),
            first
        );
    }

    /// Invalid names, nulls, duplicates and hard size limits fail before admission.
    #[test]
    fn required_mcp_rejects_malformed_and_duplicate_wire_input() {
        let mut invalid = vec![
            Value::Null,
            json!([]),
            json!({"":[]}),
            json!({"_bad":[]}),
            json!({"server":null}),
            json!({"server":["*"]}),
            json!({"server":["x","x"]}),
            json!({"server":[""]}),
            json!({"server":["tool/other"]}),
            json!({"сервер":[]}),
        ];
        invalid.push(json!({"server":(0..33).map(|n|format!("t{n}")).collect::<Vec<_>>()}));
        invalid.push(json!({"server":["x".repeat(129)]}));
        invalid.push(Value::Object(
            (0..9).map(|n| (format!("s{n}"), json!([]))).collect(),
        ));
        for requirements in invalid {
            let mut value = request();
            value["required_mcp"] = requirements;
            assert!(
                serde_json::from_value::<ProviderStartRequest>(value.clone()).is_err(),
                "{value}"
            );
        }
        let duplicate = r#"{"provider":"p","model":"m","profile":"review","task":"t","workdir":"/gone","required_mcp":{"server":[],"server":[]}}"#;
        assert!(serde_json::from_str::<ProviderStartRequest>(duplicate).is_err());
        let mut boundary = request();
        boundary["required_mcp"] = Value::Object(
            (0..8)
                .map(|server| {
                    (
                        format!("s{server}{}", "x".repeat(126)),
                        json!(
                            (0..32)
                                .map(|tool| format!("t{tool:02}{}", "x".repeat(125)))
                                .collect::<Vec<_>>()
                        ),
                    )
                })
                .collect(),
        );
        assert!(serde_json::from_value::<ProviderStartRequest>(boundary).is_ok());
        let mut valid: ProviderStartRequest = serde_json::from_value(request()).unwrap();
        valid
            .required_mcp
            .insert("server".into(), vec!["tool".into(), "tool".into()]);
        assert!(
            valid.validate_intent().is_err(),
            "direct Rust construction remains checked"
        );
    }
}
