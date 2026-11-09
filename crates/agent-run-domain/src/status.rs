//! Draft database-only status receipts; OS observation and AgentView are separate.

use crate::{
    Result, StoreId,
    batch::{ExecutionRef, PinId},
    domain::{AgentId, Status},
    error::invalid,
};
use serde::{Deserialize, Serialize};

/// Maximum selectors in one read snapshot.
pub const MAX_STATUS_TARGETS: usize = 20;

/// One exact sequence selector; missing incarnation is never called verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusSelector {
    /// Stable lineage ID or a retained execution alias.
    pub agent_id: AgentId,
    /// Exact positive execution sequence.
    pub sequence: u32,
    /// Optional opaque execution incarnation supplied by the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_ref: Option<ExecutionRef>,
    /// Optional permanent lineage retention handle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin_id: Option<PinId>,
}

/// Bounded status request; the optional view always selects the requested row.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusRequest {
    /// Request codec version; currently exactly one.
    pub version: u32,
    /// Optional database identity fence checked before returning any item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_store_id: Option<StoreId>,
    /// Selectors in requested result order.
    pub targets: Vec<StatusSelector>,
    /// Request one separately sampled selected-row view; valid for one selector.
    #[serde(default)]
    pub include_view: bool,
}

impl StatusRequest {
    /// Validates counts, view scope, positive sequence and obvious duplicate inputs.
    pub fn validate(&self) -> Result<()> {
        if self.version != 1
            || !(1..=MAX_STATUS_TARGETS).contains(&self.targets.len())
            || (self.include_view && self.targets.len() != 1)
            || self.targets.iter().any(|target| target.sequence == 0)
        {
            return Err(invalid("invalid status request"));
        }
        let mut seen = std::collections::HashSet::new();
        if self
            .targets
            .iter()
            .any(|target| !seen.insert((&target.agent_id, target.sequence)))
        {
            return Err(invalid("duplicate status target"));
        }
        Ok(())
    }
}

/// Whether the exact requested execution incarnation was established.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusMatch {
    Verified,
    Unverified,
    Missing,
    Mismatch,
    Unknown,
}

/// Relationship of the caller's pin to the canonical lineage in this snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PinState {
    Active,
    Released,
    Missing,
    Mismatch,
    Unknown,
}

/// Whether this snapshot proves complete lineage and attempt continuity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Coverage {
    Complete,
    Unknown,
}

/// Continuation after a technical service restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartResume {
    None,
    Pending,
    Unknown,
}

/// Lineage-wide cleanup proof at the same database snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupState {
    Confirmed,
    Pending,
    Unknown,
}

/// Historical successful commit evidence; never a current file or authorization proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuccessCommit {
    /// A nontechnical success and consistent answer metadata were recorded.
    Recorded,
    /// Missing, incomplete, contradictory or unverified evidence.
    Unknown,
    /// A different terminal outcome was established for the complete lineage.
    NotSucceeded,
}

/// Literal database fact about one retained execution; not an OS observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionFact {
    /// Retained sequence number.
    pub sequence: u32,
    /// Derived only from a trustworthy first provider attempt.
    pub execution_ref: Option<ExecutionRef>,
    /// Raw stored execution status, not automatically the logical lineage outcome.
    pub raw_status: Status,
}

/// Cleanup verdict is always scoped to the complete lineage, never one PID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupFact {
    /// Fixed literal `lineage`.
    pub scope: String,
    /// Conservative aggregate proof state.
    pub state: CleanupState,
}

/// One ordered selector and its bounded database-only evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusItem {
    /// Original selector, retained to correlate an unknown result.
    pub selector: StatusSelector,
    /// Resolved stable root when the snapshot establishes it.
    pub canonical_agent_id: Option<AgentId>,
    /// Exact selector verification, never a latest substitution.
    pub r#match: StatusMatch,
    /// Original retention handle, repeated alongside its state.
    pub pin_id: Option<PinId>,
    /// Observed relationship of this handle to the root.
    pub pin_state: PinState,
    /// Facts for the exact selected sequence, when unambiguous.
    pub selected: Option<ExecutionFact>,
    /// Greatest retained sequence only; latest-ever requires complete coverage.
    pub retained_latest: Option<ExecutionFact>,
    /// Proof of lineage and attempt continuity.
    pub coverage: Coverage,
    /// Technical-restart continuation state.
    pub restart_resume: RestartResume,
    /// Logical lineage terminality, or null when it is not established.
    pub lineage_terminal: Option<bool>,
    /// Terminal status only when logical terminality is established.
    pub terminal_outcome: Option<Status>,
    /// Historical DB-only success evidence; does not assert current artifact contents.
    pub success_commit: SuccessCommit,
    /// Lineage-wide database cleanup evidence.
    pub cleanup: CleanupFact,
}

/// One database snapshot and exactly one item per selector in input order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusResponse {
    /// Response codec version.
    pub version: u32,
    /// Database identity read in the same snapshot.
    pub store_id: StoreId,
    /// Unix seconds sampled once while the snapshot is held.
    pub observed_at: f64,
    /// Requested selectors in their original order.
    pub items: Vec<StatusItem>,
}

/// Redacted selected-row view; its process state is separately sampled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusView {
    /// Index in the request; views are only available for a single selector.
    pub selector_index: usize,
    /// Whether the selected incarnation was verified or remains unverified.
    pub r#match: StatusMatch,
    /// Fixed marker: status, phase, outcome and answer flag describe the selected row.
    pub status_scope: String,
    /// Unix seconds when the view and process state were sampled.
    pub sampled_at: f64,
    /// Redacted AgentView for the exact selected execution.
    pub agent: serde_json::Value,
}

/// Public status result, extending the DB proof with an optional sampled view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusOutput {
    /// Response codec version.
    pub version: u32,
    /// Database identity read with the proof.
    pub store_id: StoreId,
    /// Unix seconds sampled while the DB snapshot was held.
    pub observed_at: f64,
    /// Selector results in input order.
    pub items: Vec<StatusItem>,
    /// Optional redacted view of exactly the selected execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<StatusView>,
}

impl From<StatusResponse> for StatusOutput {
    fn from(value: StatusResponse) -> Self {
        Self {
            version: value.version,
            store_id: value.store_id,
            observed_at: value.observed_at,
            items: value.items,
            view: None,
        }
    }
}

impl StatusOutput {
    /// Checks response correlation, conservative proof states, and view redaction.
    pub fn validate_for(&self, request: &StatusRequest) -> Result<()> {
        request.validate()?;
        let bad = || invalid("invalid status response");
        if self.version != 1
            || !self.observed_at.is_finite()
            || self.observed_at < 0.0
            || self.items.len() != request.targets.len()
            || request
                .expected_store_id
                .as_ref()
                .is_some_and(|expected| expected != &self.store_id)
        {
            return Err(bad());
        }
        let mut canonical_targets = std::collections::HashSet::new();
        for (item, target) in self.items.iter().zip(&request.targets) {
            if &item.selector != target
                || item.pin_id != target.pin_id
                || item.cleanup.scope != "lineage"
            {
                return Err(bad());
            }
            if item
                .selected
                .as_ref()
                .is_some_and(|selected| selected.sequence != target.sequence)
                || (item.pin_id.is_none() && item.pin_state != PinState::Missing)
                || (matches!(
                    item.pin_state,
                    PinState::Active | PinState::Released | PinState::Mismatch
                ) && (item.pin_id.is_none() || item.canonical_agent_id.is_none()))
            {
                return Err(bad());
            }
            if item
                .canonical_agent_id
                .as_ref()
                .is_some_and(|root| !canonical_targets.insert((root, target.sequence)))
                || matches!((&item.selected, &item.retained_latest), (Some(selected), Some(tail)) if tail.sequence < selected.sequence)
            {
                return Err(bad());
            }
            match item.r#match {
                StatusMatch::Verified
                    if target.execution_ref.is_some()
                        && item
                            .selected
                            .as_ref()
                            .and_then(|v| v.execution_ref.as_ref())
                            == target.execution_ref.as_ref() => {}
                StatusMatch::Unverified
                    if target.execution_ref.is_none() && item.selected.is_some() => {}
                StatusMatch::Missing | StatusMatch::Mismatch
                    if item.selected.is_none() && item.retained_latest.is_none() => {}
                StatusMatch::Unknown => {}
                _ => return Err(bad()),
            }
            if item.coverage == Coverage::Complete
                && (item.pin_state != PinState::Active
                    || item.canonical_agent_id.is_none()
                    || !matches!(
                        item.r#match,
                        StatusMatch::Verified | StatusMatch::Unverified
                    ))
            {
                return Err(bad());
            }
            if item.lineage_terminal == Some(true)
                && (item.coverage != Coverage::Complete
                    || item.r#match != StatusMatch::Verified
                    || item.restart_resume != RestartResume::None
                    || item
                        .retained_latest
                        .as_ref()
                        .is_none_or(|v| !v.raw_status.terminal()))
            {
                return Err(bad());
            }
            if let Some(outcome) = item.terminal_outcome {
                if item.lineage_terminal != Some(true)
                    || !outcome.terminal()
                    || item
                        .retained_latest
                        .as_ref()
                        .is_none_or(|v| v.raw_status != outcome)
                {
                    return Err(bad());
                }
            } else if item.lineage_terminal == Some(true) {
                return Err(bad());
            }
            if item.cleanup.state == CleanupState::Confirmed
                && (item.coverage != Coverage::Complete
                    || item.r#match != StatusMatch::Verified
                    || item.lineage_terminal != Some(true)
                    || item.restart_resume != RestartResume::None)
            {
                return Err(bad());
            }
            match item.success_commit {
                SuccessCommit::Recorded
                    if item.lineage_terminal == Some(true)
                        && item.terminal_outcome == Some(Status::Succeeded) => {}
                SuccessCommit::NotSucceeded
                    if item.lineage_terminal == Some(true)
                        && item
                            .terminal_outcome
                            .is_some_and(|status| status != Status::Succeeded) => {}
                SuccessCommit::Unknown => {}
                _ => return Err(bad()),
            }
        }
        let eligible = request.include_view
            && self.items.len() == 1
            && matches!(
                self.items[0].r#match,
                StatusMatch::Verified | StatusMatch::Unverified
            );
        match (&self.view, eligible) {
            (None, false) => {}
            (Some(view), true) => {
                let item = &self.items[0];
                let agent: crate::AgentView =
                    serde_json::from_value(view.agent.clone()).map_err(|_| bad())?;
                if view.selector_index != 0
                    || view.r#match != item.r#match
                    || view.status_scope != "selected_execution_raw"
                    || !view.sampled_at.is_finite()
                    || view.sampled_at < 0.0
                    || agent.observed_at != view.sampled_at
                    || item.canonical_agent_id.as_ref() != Some(&agent.agent_id)
                    || item.selected.as_ref().is_none_or(|selected| {
                        selected.sequence != agent.sequence || selected.raw_status != agent.status
                    })
                    || agent.health.is_some()
                    || agent.policy.is_some()
                    || agent.cleanup.is_some()
                    || agent.failure_text.is_some()
                    || agent.delivery.last_attempt.is_some()
                    || agent.delivery.last_error.is_some()
                    || agent.delivery.orchestrator_session_id.is_some()
                    || agent.delivery.notification_id.is_some()
                    || agent
                        .answer_sha256
                        .as_ref()
                        .is_some_and(|hash| hash.parse::<crate::Sha256Digest>().is_err())
                    || agent.run_id.is_some()
                    || agent.parent_run_id.is_some()
                    || agent.parent_agent_id.is_some()
                    || agent.root_agent_id.is_some()
                    || agent.delivery.run_id.is_some()
                    || agent.delivery.agent_id != agent.agent_id
                    || agent.failure_kind.as_ref().is_some_and(|kind| {
                        kind.is_empty()
                            || kind.len() > 128
                            || !kind
                                .bytes()
                                .all(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(&byte))
                    })
                    || !matches!(
                        agent.process_state.as_str(),
                        "alive" | "dead" | "reused" | "unknown" | "denied" | "notstarted"
                    )
                {
                    return Err(bad());
                }
            }
            _ => return Err(bad()),
        }
        Ok(())
    }
}
