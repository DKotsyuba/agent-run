//! Generic, permanently replayable admission of independent agent executions.

use crate::{
    ProviderStartRequest, Result, StoreId, canonical,
    domain::{AgentId, external_id},
    error::invalid,
};
use serde::{Deserialize, Serialize};
use std::{fmt, marker::PhantomData, str::FromStr};

/// Deserializes a struct only from a map, retaining its own duplicate-key checks.
struct ObjectOnly<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for ObjectOnly<T> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct ObjectVisitor<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for ObjectVisitor<T> {
            type Value = ObjectOnly<T>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an object")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                T::deserialize(serde::de::value::MapAccessDeserializer::new(map)).map(ObjectOnly)
            }
        }
        deserializer.deserialize_map(ObjectVisitor(PhantomData))
    }
}

/// Restricts nested provider jobs without changing the legacy scalar DTO.
fn object_jobs<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<ProviderStartRequest>, D::Error> {
    Vec::<ObjectOnly<ProviderStartRequest>>::deserialize(deserializer)
        .map(|jobs| jobs.into_iter().map(|job| job.0).collect())
}

/// Uses Serde's ordinary field decoder while refusing positional struct arrays.
macro_rules! object_only_deserialize {
    ($name:ident { $($(#[$attr:meta])* $field:ident: $ty:ty),* $(,)? }) => {
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Fields { $($(#[$attr])* $field: $ty),* }
                let fields = ObjectOnly::<Fields>::deserialize(deserializer)?.0;
                Ok(Self { $($field: fields.$field),* })
            }
        }
    };
}

/// Maximum independent executions in one admission transaction.
pub const MAX_BATCH_JOBS: usize = 20;
/// Persisted canonicalization contract; old versions must remain replayable.
pub const FINGERPRINT_VERSION: u32 = 1;

/// Canonical permanent retention pin identity; release is idempotent by this key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct PinId(String);

impl PinId {
    /// Mints a candidate; the durable unique constraint prevents reuse.
    pub fn new() -> Self {
        Self(format!("pin_{}", uuid::Uuid::new_v4().simple()))
    }

    /// Returns its fixed 36-byte public spelling.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for PinId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for PinId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PinId {
    type Err = crate::Error;

    fn from_str(value: &str) -> Result<Self> {
        let raw = value
            .strip_prefix("pin_")
            .ok_or_else(|| invalid("invalid pin_id"))?;
        let uuid = uuid::Uuid::parse_str(raw).map_err(|_| invalid("invalid pin_id"))?;
        if raw.len() != 32
            || uuid.simple().to_string() != raw
            || uuid.get_version_num() != 4
            || uuid.get_variant() != uuid::Variant::RFC4122
        {
            return Err(invalid("invalid pin_id"));
        }
        Ok(Self(value.into()))
    }
}

impl<'de> Deserialize<'de> for PinId {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// Current durable release state of one history pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPinState {
    Active,
    Released,
}

/// Operator filter for a bounded history-pin page.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryPinFilter {
    #[default]
    All,
    Active,
    Released,
}

/// One shared operator operation for release and bounded observation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryPinsRequest {
    /// Idempotently release one exact durable pin.
    Release {
        /// Exact pin key.
        pin_id: PinId,
        /// Optional database scope checked in the release transaction.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_store_id: Option<StoreId>,
    },
    /// List pins after an exclusive canonical key, up to 1000 at a time.
    List {
        /// Filter both states by default.
        #[serde(default)]
        status: HistoryPinFilter,
        /// Page size, default 100 and maximum 1000.
        #[serde(default = "default_pin_limit")]
        limit: usize,
        /// Exclusive prior pin ID, independent of row retention.
        #[serde(default)]
        after_pin_id: Option<PinId>,
        /// Optional database scope checked in the same read snapshot.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_store_id: Option<StoreId>,
    },
}

fn default_pin_limit() -> usize {
    100
}

/// Public read-only view; root agent identity remains even after history expiry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryPin {
    /// Permanent pin key.
    pub pin_id: PinId,
    /// Canonical root of the retained lineage.
    pub agent_id: AgentId,
    /// Active or irreversibly released.
    pub status: HistoryPinState,
}

/// Bounded keyset page of history pins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryPinsPage {
    /// Ordered records matching the selected filter.
    pub pins: Vec<HistoryPin>,
    /// Last returned key only when more matching rows existed in this snapshot.
    pub next_after_pin_id: Option<PinId>,
}

/// Idempotent acknowledgement of one released pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryPinRelease {
    /// The requested permanent key.
    pub pin_id: PinId,
    /// Always released on a successful response.
    pub status: HistoryPinState,
    /// True when it was already released before this operation.
    pub already_released: bool,
}

/// Opaque versioned execution incarnation; an identity, not a capability or secret.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct ExecutionRef(String);

impl ExecutionRef {
    /// Derives the identity of a provider execution from its exact row and first attempt.
    /// The caller must establish that the supplied attempt belongs to this execution
    /// and has number one; a later failover attempt identifies the same execution.
    pub fn from_first_provider_attempt(execution_id: &AgentId, attempt_id: &str) -> Result<Self> {
        let raw = attempt_id
            .strip_prefix("att_")
            .ok_or_else(|| invalid("first provider attempt identity is invalid"))?;
        let uuid = uuid::Uuid::parse_str(raw)
            .map_err(|_| invalid("first provider attempt identity is invalid"))?;
        if raw.len() != 32
            || uuid.simple().to_string() != raw
            || uuid.get_version().is_none()
            || uuid.get_variant() != uuid::Variant::RFC4122
        {
            return Err(invalid("first provider attempt identity is invalid"));
        }
        let digest = canonical::sha256_hex(
            &serde_json::json!(["agent-run.execution-ref.v1", execution_id.as_str(), raw]),
            true,
        );
        Ok(Self(format!("ex1_{digest}")))
    }

    /// Returns the versioned public spelling.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExecutionRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ExecutionRef {
    type Err = crate::Error;

    fn from_str(value: &str) -> Result<Self> {
        let digest = value.strip_prefix("ex1_").unwrap_or("");
        if digest.len() == 64
            && digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            Ok(Self(value.into()))
        } else {
            Err(invalid("execution reference is invalid"))
        }
    }
}

impl<'de> Deserialize<'de> for ExecutionRef {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// Exact prior lineage required before any new member can be admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BatchPrecondition {
    /// Canonical lineage identity or a retained execution alias.
    pub agent_id: AgentId,
    /// Exact positive execution sequence.
    pub sequence: u32,
    /// Incarnation of the exact selected execution.
    pub execution_ref: ExecutionRef,
    /// Original active retention handle for the lineage.
    pub pin_id: PinId,
}

/// Validates the count and immediately visible duplicate lineage identities.
pub fn validate_preconditions(conditions: &[BatchPrecondition]) -> Result<()> {
    if !(1..=MAX_BATCH_JOBS).contains(&conditions.len())
        || conditions.iter().any(|condition| condition.sequence == 0)
        || conditions
            .iter()
            .map(|condition| &condition.agent_id)
            .collect::<std::collections::HashSet<_>>()
            .len()
            != conditions.len()
    {
        return Err(invalid("invalid batch preconditions"));
    }
    Ok(())
}

/// Decodes a present precondition list without treating null as omission.
fn present_preconditions<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Vec<BatchPrecondition>>, D::Error> {
    Vec::<BatchPrecondition>::deserialize(deserializer).map(Some)
}

/// One batch intent. Individual jobs never carry their own idempotency keys.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StartBatchRequest {
    /// Permanent key in this Run home's batch namespace.
    pub request_id: String,
    /// Independent ordinary provider requests, in receipt order.
    pub jobs: Vec<ProviderStartRequest>,
    /// Keep each new lineage until its associated pin is explicitly released.
    #[serde(default)]
    pub retain_history: bool,
    /// Optional expected database identity, fixed in the request fingerprint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_store_id: Option<StoreId>,
    /// Ordered atomic prerequisites; omission preserves the historical intent.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_preconditions"
    )]
    pub preconditions: Option<Vec<BatchPrecondition>>,
}

impl StartBatchRequest {
    /// Validates and normalizes immutable intent without reading paths or config.
    pub fn validate_intent(&mut self) -> Result<()> {
        external_id("request_id", &self.request_id)?;
        if !(1..=MAX_BATCH_JOBS).contains(&self.jobs.len()) {
            return Err(invalid("batch must contain 1..=20 jobs"));
        }
        if let Some(conditions) = &self.preconditions {
            validate_preconditions(conditions)?;
            if self.expected_store_id.is_none() {
                return Err(invalid("batch preconditions require expected_store_id"));
            }
        }
        for job in &mut self.jobs {
            if job.request_id.is_some() {
                return Err(invalid("batch jobs must not have request_id"));
            }
            job.validate_intent()?;
        }
        Ok(())
    }

    /// Hashes normalized ordered intent without depending on mutable paths or quota.
    pub fn fingerprint(&self) -> Result<String> {
        let mut intent = serde_json::json!({"version": FINGERPRINT_VERSION, "jobs": self.jobs, "retain_history": self.retain_history, "expected_store_id": self.expected_store_id});
        if let Some(conditions) = &self.preconditions {
            intent["preconditions"] = serde_json::to_value(conditions)?;
        }
        Ok(canonical::sha256_hex(&intent, true))
    }

    /// Validates the exact read-only lookup intent, requiring a database fence.
    pub fn validate_receipt_lookup(&mut self) -> Result<()> {
        self.validate_intent()?;
        if self.expected_store_id.is_none() {
            return Err(invalid("batch receipt lookup requires expected_store_id"));
        }
        Ok(())
    }

    /// Bounds a new scoped intent so its normalized lookup fits a compact MCP
    /// request even with the maximum encoded correlation ID and a trailing LF.
    /// Call only after intent normalization and existing-receipt replay.
    pub fn validate_new_lookup_frame(&self) -> Result<()> {
        if self.expected_store_id.is_none() {
            return Ok(());
        }
        let id = "x".repeat(crate::observation::MAX_RPC_ID_BYTES - 2);
        let lookup = serde_json::json!({
            "jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": {"name": "batch_receipt", "arguments": self}
        });
        if serde_json::to_vec(&lookup)?.len() + 1 > crate::observation::SOCKET_MAX_FRAME_BYTES {
            return Err(invalid("batch intent exceeds receipt lookup frame limit"));
        }
        Ok(())
    }
}

/// One stable admission identity, at the original zero-based input index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BatchJobReceipt {
    /// Position in the original request.
    pub index: usize,
    /// Stable public agent identity.
    pub agent_id: AgentId,
    /// Admission-time sequence; alone it cannot prove identity after history pruning.
    pub sequence: u32,
    /// Opaque identity of the exact admitted execution, stable across its attempts.
    pub execution_ref: ExecutionRef,
    /// Durable retention handle, present only when requested by the batch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin_id: Option<PinId>,
}

/// Immutable successful admission facts, independent of agent history retention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StartBatchReceipt {
    /// Receipt format version.
    pub version: u32,
    /// Identity of the database that committed this receipt.
    pub store_id: StoreId,
    /// Original permanent batch key.
    pub request_id: String,
    /// Every admitted identity, in original request order.
    pub jobs: Vec<BatchJobReceipt>,
    /// Exact ordered prerequisites checked at admission; omitted for old receipts.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_preconditions"
    )]
    pub preconditions: Option<Vec<BatchPrecondition>>,
}

impl StartBatchReceipt {
    /// Validates immutable receipt shape without requiring retained execution history.
    pub fn validate(&self) -> Result<()> {
        external_id("request_id", &self.request_id)?;
        if let Some(conditions) = &self.preconditions {
            validate_preconditions(conditions)?;
        }
        if self.version != 1
            || !(1..=MAX_BATCH_JOBS).contains(&self.jobs.len())
            || self
                .jobs
                .iter()
                .enumerate()
                .any(|(i, job)| job.index != i || job.sequence == 0)
            || self
                .jobs
                .iter()
                .map(|job| &job.agent_id)
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.jobs.len()
            || self
                .jobs
                .iter()
                .any(|job| job.pin_id.is_some() != self.jobs[0].pin_id.is_some())
            || self
                .jobs
                .iter()
                .filter_map(|job| job.pin_id.as_ref())
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.jobs.iter().filter(|job| job.pin_id.is_some()).count()
            || self
                .jobs
                .iter()
                .map(|job| &job.execution_ref)
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.jobs.len()
        {
            return Err(invalid("invalid batch receipt"));
        }
        Ok(())
    }
}

/// The immutable receipt and whether this call replayed its original admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StartBatchResult {
    /// Durable all-or-none admission facts.
    pub receipt: StartBatchReceipt,
    /// A replay never launches another supervisor.
    pub replayed: bool,
}

/// A permanent receipt lookup at one database snapshot.
/// A missing receipt does not rule out an in-flight admission committing later.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BatchReceiptLookup {
    /// Whether this exact intent had a committed receipt in the snapshot.
    pub found: bool,
    /// Present exactly when found; null is not a valid substitute for absence.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_receipt"
    )]
    pub receipt: Option<StartBatchReceipt>,
}

object_only_deserialize!(BatchPrecondition {
    agent_id: AgentId,
    sequence: u32,
    execution_ref: ExecutionRef,
    pin_id: PinId,
});
object_only_deserialize!(StartBatchRequest {
    request_id: String,
    #[serde(deserialize_with = "object_jobs")]
    jobs: Vec<ProviderStartRequest>,
    #[serde(default)]
    retain_history: bool,
    #[serde(default)]
    expected_store_id: Option<StoreId>,
    #[serde(default, deserialize_with = "present_preconditions")]
    preconditions: Option<Vec<BatchPrecondition>>,
});
object_only_deserialize!(BatchJobReceipt {
    index: usize,
    agent_id: AgentId,
    sequence: u32,
    execution_ref: ExecutionRef,
    #[serde(default)]
    pin_id: Option<PinId>,
});
object_only_deserialize!(StartBatchReceipt {
    version: u32,
    store_id: StoreId,
    request_id: String,
    jobs: Vec<BatchJobReceipt>,
    #[serde(default, deserialize_with = "present_preconditions")]
    preconditions: Option<Vec<BatchPrecondition>>,
});
object_only_deserialize!(StartBatchResult {
    receipt: StartBatchReceipt,
    replayed: bool,
});
object_only_deserialize!(BatchReceiptLookup {
    found: bool,
    #[serde(default, deserialize_with = "present_receipt")]
    receipt: Option<StartBatchReceipt>,
});

fn present_receipt<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<StartBatchReceipt>, D::Error> {
    StartBatchReceipt::deserialize(deserializer).map(Some)
}

impl BatchReceiptLookup {
    /// Correlates the whole result with the scoped intent and receipt shape.
    pub fn validate_for(&self, request: &StartBatchRequest) -> Result<()> {
        let mut request = request.clone();
        request.validate_receipt_lookup()?;
        match (&self.receipt, self.found) {
            (None, false) => Ok(()),
            (Some(receipt), true) => StartBatchResult {
                receipt: receipt.clone(),
                replayed: true,
            }
            .validate_for_request(&request),
            _ => Err(invalid("invalid batch receipt lookup response")),
        }
    }
}

impl StartBatchResult {
    /// Correlates a receipt to the complete normalized intent, including ordered prerequisites.
    pub fn validate_for_request(&self, request: &StartBatchRequest) -> Result<()> {
        let mut request = request.clone();
        request.validate_intent()?;
        self.validate_for(
            &request.request_id,
            request.jobs.len(),
            request.retain_history,
            request.expected_store_id.as_ref(),
        )?;
        if self.receipt.preconditions != request.preconditions {
            return Err(invalid("batch receipt preconditions do not match request"));
        }
        Ok(())
    }

    /// Validates the receipt and its correlation with the original batch intent.
    pub fn validate_for(
        &self,
        request_id: &str,
        job_count: usize,
        retain_history: bool,
        expected_store_id: Option<&StoreId>,
    ) -> Result<()> {
        self.receipt.validate()?;
        if self.receipt.request_id != request_id
            || self.receipt.jobs.len() != job_count
            || expected_store_id.is_some_and(|expected| expected != &self.receipt.store_id)
            || self
                .receipt
                .jobs
                .iter()
                .any(|job| job.pin_id.is_some() != retain_history)
        {
            return Err(invalid("batch receipt does not match request"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The reference is deterministic and changes with either source identity.
    #[test]
    fn execution_reference_is_versioned_and_validates_first_attempt() {
        let execution: AgentId = "ag-20260826-120000-0123456789".parse().unwrap();
        let other_execution: AgentId = "ag-20260826-120001-0123456789".parse().unwrap();
        let attempt = "att_550e8400e29b41d4a716446655440000";
        let reference = ExecutionRef::from_first_provider_attempt(&execution, attempt).unwrap();
        assert_eq!(
            reference.as_str(),
            "ex1_e597233542303c9e50a873fb9c9abb2c3663a685867efaabfc79322fdd268974"
        );
        assert_eq!(
            reference,
            ExecutionRef::from_first_provider_attempt(&execution, attempt).unwrap()
        );
        assert_ne!(
            reference,
            ExecutionRef::from_first_provider_attempt(&other_execution, attempt).unwrap()
        );
        assert_ne!(
            reference,
            ExecutionRef::from_first_provider_attempt(
                &execution,
                "att_550e8400e29b41d4a716446655440001"
            )
            .unwrap()
        );
        assert!(
            ExecutionRef::from_first_provider_attempt(
                &execution,
                "att_550e8400e29b11d4a716446655440000",
            )
            .is_ok()
        );
        assert!(!reference.as_str().contains(&attempt[4..]));
        for malformed in [
            "",
            "att_bad",
            "550e8400e29b41d4a716446655440000",
            "att_550E8400E29B41D4A716446655440000",
            "att_00000000000000000000000000000000",
            "att_550e8400-e29b-41d4-a716-446655440000",
        ] {
            assert!(ExecutionRef::from_first_provider_attempt(&execution, malformed).is_err());
        }
        for malformed in [
            "",
            "ex1_",
            "ex2_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "ex1_0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef",
        ] {
            assert!(serde_json::from_value::<ExecutionRef>(json!(malformed)).is_err());
        }
    }

    /// The maximum count and escaped request key fit the existing writer budget.
    #[test]
    fn maximum_batch_receipt_fits_writer_budget() {
        let mut jobs = Vec::new();
        for index in 0..MAX_BATCH_JOBS {
            let id = AgentId::new();
            let attempt = format!("att_{}", uuid::Uuid::new_v4().simple());
            jobs.push(BatchJobReceipt {
                index,
                execution_ref: ExecutionRef::from_first_provider_attempt(&id, &attempt).unwrap(),
                agent_id: id,
                sequence: 1,
                pin_id: Some(PinId::new()),
            });
        }
        let receipt = StartBatchReceipt {
            version: 1,
            store_id: "store_0123456789abcdef0123456789abcdef".parse().unwrap(),
            request_id: "😀".repeat(512),
            jobs,
            preconditions: None,
        };
        receipt.validate().unwrap();
        let output = serde_json::to_value(StartBatchResult {
            receipt,
            replayed: false,
        })
        .unwrap();
        assert!(crate::storage::encoded_len(&output) <= crate::storage::MAX_RESULT_BYTES);
    }

    /// Replay identity includes order and binding but does not inspect paths.
    #[test]
    fn intent_is_bounded_ordered_and_path_independent() {
        let job = |task: &str| {
            serde_json::from_value(json!({
                "provider":"p", "model":"m", "profile":"code", "task":task,
                "workdir":"/missing-batch-fixture"
            }))
            .unwrap()
        };
        let mut request = StartBatchRequest {
            request_id: "key".into(),
            jobs: vec![job("one"), job("two")],
            retain_history: false,
            expected_store_id: None,
            preconditions: None,
        };
        request.validate_intent().unwrap();
        let fingerprint = request.fingerprint().unwrap();
        assert_eq!(
            fingerprint,
            "2bb8c1d35e1e28d3467dc4bda6b8cbd4c3391ad940df28d52b0cae44dd50608e"
        );
        assert!(
            serde_json::to_value(&request)
                .unwrap()
                .get("preconditions")
                .is_none()
        );
        request.jobs.swap(0, 1);
        assert_ne!(request.fingerprint().unwrap(), fingerprint);
        request.jobs.swap(0, 1);
        request.retain_history = true;
        assert_ne!(request.fingerprint().unwrap(), fingerprint);
        let unscoped = request.fingerprint().unwrap();
        request.expected_store_id = Some("store_0123456789abcdef0123456789abcdef".parse().unwrap());
        assert_ne!(request.fingerprint().unwrap(), unscoped);
        request.jobs[0].request_id = Some("individual".into());
        assert!(request.validate_intent().is_err());
        request.jobs = Vec::new();
        assert!(request.validate_intent().is_err());
        request.jobs = vec![job("one"); MAX_BATCH_JOBS + 1];
        assert!(request.validate_intent().is_err());
    }

    /// Preconditions are ordered immutable intent, never a nullable default.
    #[test]
    fn batch_preconditions_are_strict_and_correlate_the_receipt() {
        let condition = |second: bool| {
            json!({
                "agent_id":if second {"ag-20260826-120001-0123456789"} else {"ag-20260826-120000-0123456789"},
                "sequence":1,"execution_ref":format!("ex1_{}",(if second {"b"} else {"a"}).repeat(64)),
                "pin_id":if second {"pin_550e8400e29b41d4a716446655440001"} else {"pin_550e8400e29b41d4a716446655440000"}
            })
        };
        let base = json!({"request_id":"key","expected_store_id":"store_0123456789abcdef0123456789abcdef",
            "jobs":[{"provider":"p","model":"m","profile":"review","task":"fixture","workdir":"/missing"}]});
        let mut invalid_null = base.clone();
        invalid_null["preconditions"] = json!(null);
        assert!(serde_json::from_value::<StartBatchRequest>(invalid_null).is_err());
        let mut invalid_empty = base.clone();
        invalid_empty["preconditions"] = json!([]);
        assert!(
            serde_json::from_value::<StartBatchRequest>(invalid_empty)
                .unwrap()
                .validate_intent()
                .is_err()
        );
        let mut no_scope = base.clone();
        no_scope
            .as_object_mut()
            .unwrap()
            .remove("expected_store_id");
        no_scope["preconditions"] = json!([condition(false)]);
        assert!(
            serde_json::from_value::<StartBatchRequest>(no_scope)
                .unwrap()
                .validate_intent()
                .is_err()
        );
        let mut input = base.clone();
        input["preconditions"] = json!([condition(false), condition(true)]);
        let mut request: StartBatchRequest = serde_json::from_value(input.clone()).unwrap();
        request.validate_intent().unwrap();
        let fingerprint = request.fingerprint().unwrap();
        request.preconditions.as_mut().unwrap().swap(0, 1);
        assert_ne!(request.fingerprint().unwrap(), fingerprint);
        request.preconditions.as_mut().unwrap().swap(0, 1);
        let mut duplicate = request.clone();
        let first_id = duplicate.preconditions.as_ref().unwrap()[0]
            .agent_id
            .clone();
        duplicate.preconditions.as_mut().unwrap()[1].agent_id = first_id;
        duplicate.preconditions.as_mut().unwrap()[1].sequence = 2;
        assert!(duplicate.validate_intent().is_err());
        let mut zero = input.clone();
        zero["preconditions"][0]["sequence"] = json!(0);
        assert!(
            serde_json::from_value::<StartBatchRequest>(zero)
                .unwrap()
                .validate_intent()
                .is_err()
        );
        let mut excessive = input;
        excessive["preconditions"] = json!(vec![condition(false); MAX_BATCH_JOBS + 1]);
        assert!(
            serde_json::from_value::<StartBatchRequest>(excessive)
                .unwrap()
                .validate_intent()
                .is_err()
        );
        let id = AgentId::new();
        let receipt = StartBatchResult {
            receipt: StartBatchReceipt {
                version: 1,
                store_id: request.expected_store_id.clone().unwrap(),
                request_id: request.request_id.clone(),
                jobs: vec![BatchJobReceipt {
                    index: 0,
                    execution_ref: ExecutionRef::from_first_provider_attempt(
                        &id,
                        "att_550e8400e29b41d4a716446655440000",
                    )
                    .unwrap(),
                    agent_id: id,
                    sequence: 1,
                    pin_id: None,
                }],
                preconditions: request.preconditions.clone(),
            },
            replayed: false,
        };
        receipt.validate_for_request(&request).unwrap();
        let mut wrong = receipt.clone();
        wrong.receipt.preconditions.as_mut().unwrap().swap(0, 1);
        assert!(wrong.validate_for_request(&request).is_err());
        let mut encoded = serde_json::to_value(&receipt.receipt).unwrap();
        encoded["preconditions"] = json!(null);
        assert!(serde_json::from_value::<StartBatchReceipt>(encoded).is_err());
    }

    /// Twenty distinct valid prerequisites fit; twenty-one fail only the count bound.
    #[test]
    fn batch_precondition_count_accepts_twenty_distinct_and_rejects_twenty_one() {
        let conditions: Vec<_> = (0..21)
            .map(|index| {
                let agent_id: AgentId = format!("ag-20260826-120000-{index:010x}").parse().unwrap();
                let pin_id: PinId = format!("pin_550e8400e29b41d4a716{index:012x}")
                    .parse()
                    .unwrap();
                let execution_ref = ExecutionRef::from_first_provider_attempt(
                    &agent_id,
                    "att_550e8400e29b41d4a716446655440000",
                )
                .unwrap();
                BatchPrecondition {
                    agent_id,
                    sequence: 1,
                    execution_ref,
                    pin_id,
                }
            })
            .collect();
        assert_eq!(
            conditions
                .iter()
                .map(|c| &c.agent_id)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            21
        );
        assert_eq!(
            conditions
                .iter()
                .map(|c| &c.pin_id)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            21
        );
        for condition in &conditions {
            validate_preconditions(std::slice::from_ref(condition)).unwrap();
        }
        let mut request: StartBatchRequest = serde_json::from_value(json!({
            "request_id":"count-boundary","expected_store_id":"store_0123456789abcdef0123456789abcdef",
            "jobs":[{"provider":"p","model":"m","profile":"review","task":"fixture","workdir":"/missing"}]
        })).unwrap();
        request.preconditions = Some(conditions[..20].to_vec());
        request.validate_intent().unwrap();
        request.preconditions = Some(conditions[1..].to_vec());
        request.validate_intent().unwrap();
        request.preconditions = Some(conditions);
        assert!(request.validate_intent().is_err());
    }

    /// Wire structs and nested jobs are objects, while duplicate field checks remain strict.
    #[test]
    fn batch_wire_objects_reject_positional_arrays_and_duplicate_fields() {
        let condition = json!({"agent_id":"ag-20260826-120000-0123456789","sequence":1,
            "execution_ref":format!("ex1_{}","a".repeat(64)),"pin_id":"pin_550e8400e29b41d4a716446655440000"});
        let job = json!({"provider":"p","model":"m","profile":"review","task":"fixture","workdir":"/missing"});
        let request = json!({"request_id":"key","expected_store_id":"store_0123456789abcdef0123456789abcdef",
            "jobs":[job],"preconditions":[condition]});
        let mut typed: StartBatchRequest = serde_json::from_value(request.clone()).unwrap();
        typed.validate_intent().unwrap();
        let mut bad = request.clone();
        bad["preconditions"][0] = json!([
            condition["agent_id"],
            1,
            condition["execution_ref"],
            condition["pin_id"]
        ]);
        assert!(serde_json::from_value::<StartBatchRequest>(bad).is_err());
        let mut bad = request.clone();
        bad["jobs"][0] = json!(["p", "m", "review", "fixture", "/missing"]);
        assert!(serde_json::from_value::<StartBatchRequest>(bad).is_err());
        assert!(
            serde_json::from_value::<StartBatchRequest>(json!(["key", [], false, null, null]))
                .is_err()
        );
        let duplicate = request
            .to_string()
            .replace("\"sequence\":1", "\"sequence\":2,\"sequence\":1");
        assert!(serde_json::from_str::<StartBatchRequest>(&duplicate).is_err());
        let receipt = json!({"version":1,"store_id":"store_0123456789abcdef0123456789abcdef","request_id":"key",
            "preconditions":[condition],"jobs":[{"index":0,"agent_id":"ag-20260826-120001-0123456789","sequence":1,"execution_ref":format!("ex1_{}","b".repeat(64))}]});
        let result = json!({"receipt":receipt,"replayed":false});
        serde_json::from_value::<StartBatchResult>(result.clone())
            .unwrap()
            .validate_for_request(&typed)
            .unwrap();
        let mut bad = result.clone();
        bad["receipt"]["jobs"][0] = json!([
            0,
            "ag-20260826-120001-0123456789",
            1,
            format!("ex1_{}", "b".repeat(64)),
            null
        ]);
        assert!(serde_json::from_value::<StartBatchResult>(bad).is_err());
        let mut bad = result.clone();
        bad["receipt"]["preconditions"][0] = json!([
            condition["agent_id"],
            1,
            condition["execution_ref"],
            condition["pin_id"]
        ]);
        assert!(serde_json::from_value::<StartBatchResult>(bad).is_err());
        let mut bad = result.clone();
        bad["receipt"] = json!([
            1,
            receipt["store_id"],
            "key",
            receipt["jobs"],
            receipt["preconditions"]
        ]);
        assert!(serde_json::from_value::<StartBatchResult>(bad).is_err());
        assert!(serde_json::from_value::<StartBatchResult>(json!([receipt, false])).is_err());
        assert!(serde_json::from_value::<BatchReceiptLookup>(json!([false, null])).is_err());
    }
}
