//! Read-only discovery and execution-fenced answer paging contracts.

use crate::{Result, Sha256Digest, domain::AgentId, error::invalid};
use serde::{Deserialize, Serialize};

/// Default requested answer payload bytes, before JSON escaping.
pub const DEFAULT_CHUNK_BYTES: usize = 64 * 1024;
/// Maximum payload bytes; sixfold JSON escaping still fits the socket frame.
pub const MAX_CHUNK_BYTES: usize = 128 * 1024;
/// Complete newline-terminated socket frame ceiling shared with discovery.
pub const SOCKET_MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Maximum serialized correlation ID accepted by the bounded public transports.
pub const MAX_RPC_ID_BYTES: usize = 1024;

/// Optional opt-in to reading the current database identity.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilitiesQuery {
    /// False keeps discovery independent of the database.
    #[serde(default)]
    pub include_store_id: bool,
    /// Opt in to discovery of operator-only lineage-hold socket methods.
    #[serde(default)]
    pub include_lineage_holds: bool,
    /// Opt in to discovery of the `initial_task` read method, its modes and bounds.
    #[serde(default)]
    pub include_initial_task: bool,
}

/// Strict current-execution selection and byte cursor; never accepts a path.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnswerChunkQuery {
    /// Stable agent identity or retained alias for its lineage.
    pub agent_id: AgentId,
    /// Required positive current execution receipt; old executions are rejected.
    pub sequence: std::num::NonZeroU32,
    /// Required exact lowercase SHA-256 of the sealed artifact.
    pub expected_sha256: Sha256Digest,
    /// Nonnegative byte offset on a UTF-8 boundary, at most the artifact size.
    pub offset_bytes: u64,
    /// Payload budget in 1..=128 KiB; omission defaults to 64 KiB, null rejects.
    #[serde(default = "default_chunk_bytes")]
    pub max_bytes: usize,
}

/// Returns the transport-independent omitted chunk budget.
pub fn default_chunk_bytes() -> usize {
    DEFAULT_CHUNK_BYTES
}

impl AnswerChunkQuery {
    /// Rejects zero and excessive budgets before opening any store or artifact.
    pub fn validate(&self) -> Result<()> {
        if !(1..=MAX_CHUNK_BYTES).contains(&self.max_bytes) {
            return Err(invalid("max_bytes must be in 1..=131072"));
        }
        Ok(())
    }
}

/// Verified exact artifact bytes, including a legacy terminal frame if present.
/// Cursor units are UTF-8 bytes; no lifecycle state is changed by this read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnswerChunkView {
    /// Canonical stable lineage identity.
    pub agent_id: AgentId,
    /// Exact current execution receipt, revalidated before returning.
    pub sequence: u32,
    /// SHA-256 over all exact artifact bytes, not merely this chunk.
    pub sha256: String,
    /// Total verified artifact bytes, at most 16 MiB.
    pub total_bytes: u64,
    /// Requested UTF-8 byte boundary.
    pub offset_bytes: u64,
    /// Exclusive end boundary to use as the next request's offset.
    pub next_offset: u64,
    /// Exact bounded UTF-8 substring; empty only at EOF.
    pub content: String,
    /// Whether next_offset equals total_bytes; EOF itself returns true.
    pub complete: bool,
}

/// One actual read method from the sole domain registry, including legacy calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadMethodView {
    /// Exact shared dispatch name.
    pub name: String,
    /// Authoritative argument JSON Schema.
    pub input_schema: serde_json::Value,
    /// Result declaration when registered; null for undeclared historical shapes.
    pub result_shape: Option<serde_json::Value>,
}

/// Compiled bounds on structured observation responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationLimits {
    /// Complete newline-terminated socket frame bytes.
    pub socket_max_frame_bytes: usize,
    /// Maximum sealed artifact bytes accepted by verification.
    pub answer_max_bytes: usize,
    /// Omitted chunk request budget in raw UTF-8 bytes.
    pub answer_chunk_default_bytes: usize,
    /// Maximum chunk request budget before JSON escaping.
    pub answer_chunk_max_bytes: usize,
}

/// Versioned compiled discovery; never claims installed revision or qualification.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilitiesView {
    /// Response contract version, currently one.
    pub schema_version: u32,
    /// Product name, agent-run.
    pub product: String,
    /// Current database identity only when explicitly requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_id: Option<crate::StoreId>,
    /// Compiled Cargo package version, independent of protocol versions.
    pub version: String,
    /// Verified build source identity, null when no build input supplies it.
    pub source_revision: Option<String>,
    /// Available public transport names: cli, mcp and unix_socket.
    pub transports: Vec<String>,
    /// Compiled answer and wire bounds.
    pub limits: ObservationLimits,
    /// Actual read-only registry names and argument/result declarations.
    pub read_methods: Vec<ReadMethodView>,
    /// Present only when `include_initial_task` was requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_task: Option<crate::initial_task::InitialTaskCapability>,
}
