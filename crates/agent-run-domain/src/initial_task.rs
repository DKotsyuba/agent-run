//! Exact initial-task read of a retained root execution; content is data, never authority.

use crate::{
    Result, Sha256Digest, StoreId,
    batch::{ExecutionRef, PinId},
    domain::{AgentId, MAX_TASK_BYTES as MAX_SOURCE_BYTES},
    error::invalid,
};
use serde::{Deserialize, Serialize};

/// Wire codec version, currently exactly one.
pub const VERSION: u32 = 1;
/// Largest task accepted for export; a larger stored source is unavailable, never cut.
pub const MAX_TASK_BYTES: usize = MAX_SOURCE_BYTES;
/// Largest and default UTF-8 payload bytes per page; sixfold JSON escaping fits one frame.
pub const MAX_PAGE_BYTES: usize = 64 * 1024;
/// Smallest page budget; one UTF-8 scalar is at most four bytes, so paging always advances.
pub const MIN_PAGE_BYTES: usize = 4;

/// Evidence required before the stored task is returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitialTaskMode {
    /// Store, root execution, execution ref, active history pin and complete coverage.
    Strict,
    /// Current retained root execution only; no pin, coverage or latest-ever claim.
    Current,
}

/// Optional fields may be omitted, but explicit null is never a continuation token.
fn present_optional<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

/// Requires the field to be present while retaining explicit JSON null.
fn required_nullable<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Omitted page budget.
fn default_max_bytes() -> usize {
    MAX_PAGE_BYTES
}

/// Compiled support declaration returned by `capabilities` with `include_initial_task`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialTaskCapability {
    /// Wire codec version of the `initial_task` method.
    pub version: u32,
    /// Registry method name shared by CLI, MCP and socket JSON-RPC.
    pub method: String,
    /// Supported evidence modes.
    pub modes: Vec<InitialTaskMode>,
    /// Largest exportable task in UTF-8 bytes.
    pub max_task_bytes: usize,
    /// Largest and default page payload in UTF-8 bytes.
    pub max_page_bytes: usize,
}

impl InitialTaskCapability {
    /// The one capability this build supports.
    pub fn current() -> Self {
        Self {
            version: VERSION,
            method: "initial_task".into(),
            modes: vec![InitialTaskMode::Strict, InitialTaskMode::Current],
            max_task_bytes: MAX_TASK_BYTES,
            max_page_bytes: MAX_PAGE_BYTES,
        }
    }
}

/// One page request. Start with offset 0 and no fences; continue with the returned
/// `next_offset`, repeating `expected_sha256` and `expected_total_bytes` of the first page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialTaskRequest {
    /// Wire codec version, currently exactly one.
    pub version: u32,
    /// Evidence mode; the response never claims more than this mode proves.
    pub mode: InitialTaskMode,
    /// Database identity fence; required for `strict`, optional for `current`.
    #[serde(
        default,
        deserialize_with = "present_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub expected_store_id: Option<StoreId>,
    /// Canonical root agent ID; a resume alias is unavailable, never normalized.
    pub agent_id: AgentId,
    /// Execution sequence; required for `strict`; only 1 (root) can be available.
    #[serde(
        default,
        deserialize_with = "present_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub sequence: Option<u32>,
    /// Incarnation receipt of the first provider attempt; required for `strict`,
    /// an optional per-page identity fence for `current`.
    #[serde(
        default,
        deserialize_with = "present_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_ref: Option<ExecutionRef>,
    /// Active permanent retention handle; required for `strict`, rejected for `current`.
    #[serde(
        default,
        deserialize_with = "present_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub pin_id: Option<PinId>,
    /// UTF-8 byte offset of the page; must be a character boundary.
    #[serde(default)]
    pub offset: usize,
    /// Page budget in bytes, 4..=65536; the page ends at a character boundary.
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,
    /// Whole-text SHA-256 from an earlier page; required with `expected_total_bytes`
    /// for a nonzero offset.
    #[serde(
        default,
        deserialize_with = "present_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub expected_sha256: Option<Sha256Digest>,
    /// Whole-text length from an earlier page; paired with `expected_sha256`.
    #[serde(
        default,
        deserialize_with = "present_optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub expected_total_bytes: Option<usize>,
}

impl InitialTaskRequest {
    /// Rejects ambiguous modes and continuations before any store content is read.
    pub fn validate(&self) -> Result<()> {
        let bad = || invalid("invalid initial task request");
        let strict = self.mode == InitialTaskMode::Strict;
        if self.version != VERSION
            || self.sequence == Some(0)
            || (strict
                && (self.expected_store_id.is_none()
                    || self.sequence.is_none()
                    || self.execution_ref.is_none()
                    || self.pin_id.is_none()))
            || (!strict && self.pin_id.is_some())
            || !(MIN_PAGE_BYTES..=MAX_PAGE_BYTES).contains(&self.max_bytes)
            || self.expected_sha256.is_some() != self.expected_total_bytes.is_some()
            || (self.offset != 0 && self.expected_sha256.is_none())
            || self.offset >= MAX_TASK_BYTES
            || self
                .expected_total_bytes
                .is_some_and(|total| total == 0 || total > MAX_TASK_BYTES || self.offset >= total)
        {
            return Err(bad());
        }
        Ok(())
    }

    /// The root execution sequence this request selects.
    pub fn sequence(&self) -> u32 {
        self.sequence.unwrap_or(1)
    }
}

/// Closed reasons for which the exact text is withheld.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitialTaskReason {
    /// No retained execution has this ID (absent, or removed without a pin tombstone).
    NotFound,
    /// `strict` only: the pin tombstone exists but retention removed the lineage rows.
    RetentionDeleted,
    /// The ID is not a canonical root, or the sequence is not 1.
    NotRootExecution,
    /// The first provider attempt is missing or cannot yield an execution ref.
    ExecutionRefUnavailable,
    /// The supplied execution ref differs from the one computed in this snapshot.
    ExecutionRefMismatch,
    /// `strict` only: the pin does not exist.
    PinMissing,
    /// `strict` only: the pin was released; its tombstone grants no retention.
    PinReleased,
    /// `strict` only: the pin belongs to a different root.
    PinMismatch,
    /// `strict` only: lineage or attempt continuity is not proven complete.
    CoverageIncomplete,
    /// A task source is absent, not text, blank, unparsable or over its read budget.
    TaskMissing,
    /// `agents.task`, `request_json.task` and the frozen provider task disagree.
    TaskMismatch,
    /// A task source exceeds 512 KiB.
    TaskTooLarge,
    /// The frozen provider-v2 identity of the first execution does not verify.
    ProviderIdentityInvalid,
    /// The supplied whole-text SHA-256 or length differs from the stored text.
    ContentChanged,
}

impl InitialTaskReason {
    /// Reasons that only the strict proof can produce.
    fn strict_only(self) -> bool {
        matches!(
            self,
            Self::RetentionDeleted
                | Self::PinMissing
                | Self::PinReleased
                | Self::PinMismatch
                | Self::CoverageIncomplete
        )
    }
}

/// One verified page of the complete text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialTaskPage {
    /// Wire codec version, currently exactly one.
    pub version: u32,
    /// Evidence mode actually applied.
    pub mode: InitialTaskMode,
    /// Store identity observed in this page's read snapshot.
    pub store_id: StoreId,
    /// Canonical root agent ID.
    pub agent_id: AgentId,
    /// Always 1, the root execution.
    pub sequence: u32,
    /// Execution ref computed from the first provider attempt in this snapshot.
    pub execution_ref: ExecutionRef,
    /// The active pin, only in `strict`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin_id: Option<PinId>,
    /// Whole-text length in UTF-8 bytes, 1..=512 KiB.
    pub total_bytes: usize,
    /// SHA-256 of the whole text; identical on every page of one text.
    pub sha256: Sha256Digest,
    /// Inclusive UTF-8 byte offset of `text`.
    pub offset: usize,
    /// Exclusive UTF-8 byte offset of `text`.
    pub end: usize,
    /// Offset to request next, or null when this page ends the text.
    #[serde(deserialize_with = "required_nullable")]
    pub next_offset: Option<usize>,
    /// Exact slice of the stored task, split only at UTF-8 boundaries.
    pub text: String,
    /// True when `end == total_bytes`.
    pub complete: bool,
}

/// Typed refusal; no text, fragment or prefix accompanies it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialTaskUnavailable {
    /// Wire codec version, currently exactly one.
    pub version: u32,
    /// Evidence mode applied.
    pub mode: InitialTaskMode,
    /// Store identity observed in this snapshot.
    pub store_id: StoreId,
    /// Requested agent ID, repeated for correlation.
    pub agent_id: AgentId,
    /// Why the text is withheld.
    pub reason: InitialTaskReason,
}

/// Either an exact page or a typed unavailable state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum InitialTaskResponse {
    /// An exact, size- and identity-checked page of the stored task.
    Available(InitialTaskPage),
    /// The text is withheld for a closed reason.
    Unavailable(InitialTaskUnavailable),
}

impl InitialTaskResponse {
    /// Checks correlation and paging arithmetic before a transport publishes data.
    /// Clients must also buffer every page and verify length and SHA-256 over the whole text.
    pub fn validate_for(&self, request: &InitialTaskRequest) -> Result<()> {
        request.validate()?;
        let bad = || invalid("invalid initial task response");
        match self {
            Self::Unavailable(value) => {
                if value.version != VERSION
                    || value.mode != request.mode
                    || value.agent_id != request.agent_id
                    || request
                        .expected_store_id
                        .as_ref()
                        .is_some_and(|id| id != &value.store_id)
                    || (value.reason.strict_only() && request.mode != InitialTaskMode::Strict)
                    || (value.reason == InitialTaskReason::ExecutionRefMismatch
                        && request.execution_ref.is_none())
                    || (value.reason == InitialTaskReason::ContentChanged
                        && request.expected_sha256.is_none())
                {
                    return Err(bad());
                }
            }
            Self::Available(page) => {
                let more = page.end < page.total_bytes;
                if page.version != VERSION
                    || page.mode != request.mode
                    || page.agent_id != request.agent_id
                    || page.sequence != 1
                    || request.sequence() != 1
                    || request
                        .expected_store_id
                        .as_ref()
                        .is_some_and(|id| id != &page.store_id)
                    || request
                        .execution_ref
                        .as_ref()
                        .is_some_and(|value| value != &page.execution_ref)
                    || page.pin_id != request.pin_id
                    || page.total_bytes == 0
                    || page.total_bytes > MAX_TASK_BYTES
                    || page.offset != request.offset
                    || page.offset >= page.end
                    || page.end > page.total_bytes
                    || page.end - page.offset != page.text.len()
                    || page.text.len() > request.max_bytes
                    || page.complete == more
                    || page.next_offset != more.then_some(page.end)
                    || request
                        .expected_sha256
                        .as_ref()
                        .is_some_and(|value| value != &page.sha256)
                    || request
                        .expected_total_bytes
                        .is_some_and(|value| value != page.total_bytes)
                {
                    return Err(bad());
                }
            }
        }
        Ok(())
    }
}
