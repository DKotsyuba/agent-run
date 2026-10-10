//! Strict provider start input, distinct from readable historical runtime requests.

use crate::{
    Result,
    catalog::ProviderId,
    domain::{Constraint, OrchestratorRef, StartRequest},
    types::AccountLabel,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf};

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
    /// Validates paths, task, effort, namespace, orchestrator transport
    /// and the optional human label; stores canonical paths, labels and transport
    /// in place. Invalid values return `ValidationError` before admission.
    /// Historical runtime selectors remain excluded from public v2 input.
    pub fn validate(&mut self) -> Result<()> {
        let mut projection = self.storage_projection();
        projection.validate()?;
        self.workdir = projection.workdir;
        self.read_roots = projection.read_roots;
        self.display_name = projection.display_name;
        self.orchestrator = projection.orchestrator;
        Ok(())
    }

    /// Validates immutable fields for exact replay without requiring live directories.
    /// A new request still calls `validate` before admission and canonicalizes paths.
    pub fn validate_intent(&mut self) -> Result<()> {
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
