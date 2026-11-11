//! Bounded public status using one database snapshot and an optional sampled view.

use crate::{Error, Result, agent_identity, domain::now, service::Service};
use agent_run_domain::status::{StatusMatch, StatusOutput, StatusRequest, StatusView};
use agent_run_store::{status::read_in_snapshot, writer::StoreReader};
use serde::Serialize;
use std::io::{self, Write};

/// Reserves space for a bounded JSON-RPC ID and MCP envelope within one frame.
const MAX_STATUS_BYTES: usize = agent_run_domain::observation::SOCKET_MAX_FRAME_BYTES - 8192;
/// Reuses the largest existing writer admission bound before loading a view.
const MAX_VIEW_SOURCE_BYTES: i64 = 16 * 1024 * 1024;

/// Counts encoded bytes without publishing or retaining a partial response.
struct SizeBudget {
    remaining: usize,
}

impl Write for SizeBudget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(io::Error::other("status response exceeds frame budget"));
        }
        self.remaining -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Rejects a complete response that cannot fit the shared transport budgets.
fn check_budget<T: Serialize>(value: &T) -> Result<()> {
    serde_json::to_writer(
        &mut SizeBudget {
            remaining: MAX_STATUS_BYTES,
        },
        value,
    )
    .map_err(|_| Error::Runtime("status response exceeds frame budget".into()))
}

/// Builds the selected AgentView without forwarding opaque or native process details.
fn selected_view(
    store: &StoreReader,
    id: &agent_run_domain::domain::AgentId,
    sampled_at: f64,
) -> Result<serde_json::Value> {
    let safe_to_read: bool = store.conn().query_row(
        "SELECT COALESCE(length(CAST(request_json AS BLOB)),0)+COALESCE(length(CAST(identity_json AS BLOB)),0)+ \
         COALESCE(length(CAST(task AS BLOB)),0)+COALESCE(length(CAST(task_summary AS BLOB)),0)+ \
         COALESCE(length(CAST(failure_text AS BLOB)),0)+COALESCE(length(CAST(workdir AS BLOB)),0)<=?2 \
         FROM agents WHERE id=?1", rusqlite::params![id.as_str(),MAX_VIEW_SOURCE_BYTES], |row|row.get(0),
    )?;
    if !safe_to_read {
        return Err(Error::Runtime("status view source exceeds budget".into()));
    }
    let mut view = store.agent_view_at(id, sampled_at)?;
    // These dynamic documents can carry native identifiers or untrusted text.
    // The lineage cleanup proof is separately present in StatusItem.
    view.health = None;
    view.policy = None;
    view.cleanup = None;
    view.failure_text = None;
    view.delivery.last_attempt = None;
    view.delivery.last_error = None;
    view.delivery.orchestrator_session_id = None;
    view.delivery.notification_id = None;
    view.answer_sha256 = view
        .answer_sha256
        .filter(|hash| hash.parse::<agent_run_domain::Sha256Digest>().is_ok());
    view.failure_kind = view.failure_kind.filter(|kind| {
        !kind.is_empty()
            && kind.len() <= 128
            && kind
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(&byte))
    });
    agent_identity::view(serde_json::to_value(view)?)
}

/// Builds proof and optional selected view while the same read snapshot is open.
fn read_status(store: &StoreReader, request: &StatusRequest) -> Result<StatusOutput> {
    let tx = store.conn().unchecked_transaction()?;
    let output = {
        let snapshot = read_in_snapshot(&tx, request)?;
        let eligible = request.include_view
            && matches!(
                snapshot.response.items[0].r#match,
                StatusMatch::Verified | StatusMatch::Unverified
            );
        let selected = eligible
            .then(|| snapshot.selected_execution(0).cloned())
            .flatten();
        let mut output = StatusOutput::from(snapshot.response);
        if eligible {
            let selected =
                selected.ok_or_else(|| Error::Runtime("status selected row unavailable".into()))?;
            let sampled_at = now();
            output.view = Some(StatusView {
                selector_index: 0,
                r#match: output.items[0].r#match,
                status_scope: "selected_execution_raw".into(),
                sampled_at,
                agent: selected_view(store, &selected, sampled_at)?,
            });
        }
        output
            .validate_for(request)
            .map_err(|_| Error::Runtime("status response is inconsistent".into()))?;
        check_budget(&output)?;
        output
    };
    tx.commit()?;
    Ok(output)
}

impl Service {
    /// Reads exact status selectors without mutating state or opening answer files.
    pub async fn status(&self, request: StatusRequest) -> Result<StatusOutput> {
        request.validate()?;
        self.read(move |store| read_status(store, &request)).await
    }
}

#[cfg(test)]
mod tests {
    /// Encoded size is checked as JSON, including escaping, without truncation.
    #[test]
    fn status_budget_counts_json_escaping() {
        let value = serde_json::json!({"value":"\u{1}".repeat(super::MAX_STATUS_BYTES / 6 + 1)});
        assert!(super::check_budget(&value).is_err());
        assert!(super::check_budget(&serde_json::json!({"value":"ok"})).is_ok());
    }
}
