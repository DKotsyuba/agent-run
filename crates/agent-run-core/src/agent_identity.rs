//! Stable public agent identities over immutable execution records.
//!
//! Storage, supervisors and delivery leases continue to address exact run rows.
//! Public callers use the lineage root and may pin a historical `run_id`.

use crate::{domain::AgentId, error::invalid, state::Record, state::Store, Error, Result};
use rusqlite::OptionalExtension;
use serde_json::{json, Value};

/// Resolve a stable agent or historical alias to its latest run, or to `run_id`.
///
/// Every old run id remains an alias for its immutable lineage root. An explicit
/// run must belong to that lineage. Missing identifiers return NotFound and a
/// foreign run returns ValidationError. Resolution never writes state or follows
/// a provider's native session id; callers retain the returned exact run for the
/// entire operation so a concurrent resume cannot redirect it later.
pub fn resolve(store: &Store, agent_id: &AgentId, run_id: Option<&AgentId>) -> Result<Record> {
    let root = match store.get(agent_id) {
        Ok(row) => row.root_agent_id,
        // Retention may remove the original row while later lineage rows live.
        Err(Error::NotFound(_)) => agent_id.clone(),
        Err(error) => return Err(error),
    };
    if let Some(run_id) = run_id {
        let row = store.get(run_id)?;
        if row.root_agent_id != root {
            return Err(invalid("run_id does not belong to agent_id"));
        }
        return Ok(row);
    }
    let latest: Option<String> = store
        .conn
        .query_row(
            "SELECT id FROM agents WHERE root_agent_id=?1 OR id=?1 \
             ORDER BY sequence DESC,created_at DESC,id DESC LIMIT 1",
            [root.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    store.get(
        &latest
            .ok_or_else(|| Error::NotFound(agent_id.to_string()))?
            .parse()?,
    )
}

/// Attach public identity to a known run result without traversing user content.
///
/// Only the envelope and its delivery metadata are changed. Answers, transcript
/// messages, policy and arbitrary tool content remain byte-for-byte untouched.
pub fn result(row: &Record, mut value: Value) -> Result<Value> {
    identify(&mut value, &row.root_agent_id, &row.id)?;
    if value.get("parent_agent_id").is_some() {
        if let Some(parent) = &row.parent_agent_id {
            value["parent_run_id"] = json!(parent);
        }
    }
    Ok(value)
}

/// Convert an exact-run view into a stable public view while retaining lineage.
pub fn view(mut value: Value) -> Result<Value> {
    let run: AgentId = serde_json::from_value(value["agent_id"].clone())?;
    let root: AgentId = serde_json::from_value(value["root_agent_id"].clone())?;
    identify(&mut value, &root, &run)?;
    if value["parent_agent_id"].is_string() {
        value["parent_run_id"] = value["parent_agent_id"].clone();
    }
    Ok(value)
}

/// Project a fully admitted run only after the supervisor handoff used its id.
///
/// The exact execution id remains available for hooks and observer pinning;
/// replay and new admission use the same envelope.
pub fn admission(mut value: Value) -> Result<Value> {
    value["agent"] = view(value["agent"].take())?;
    let root = serde_json::from_value(value["agent"]["agent_id"].clone())?;
    let run = serde_json::from_value(value["agent"]["run_id"].clone())?;
    identify(&mut value, &root, &run)?;
    Ok(value)
}

/// Add the two identities to one product-owned object and its delivery receipt.
fn identify(value: &mut Value, root: &AgentId, run: &AgentId) -> Result<()> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| invalid("run result must be an object"))?;
    object.insert("agent_id".into(), json!(root));
    object.insert("run_id".into(), json!(run));
    if let Some(Value::Object(delivery)) = object.get_mut("delivery") {
        delivery.insert("agent_id".into(), json!(root));
        delivery.insert("run_id".into(), json!(run));
    }
    Ok(())
}

impl crate::service::Service {
    /// Resolve a public selection against this home without launching anything.
    pub fn resolve_run(&self, agent_id: &AgentId, run_id: Option<&AgentId>) -> Result<Record> {
        resolve(&Store::open(&self.home)?, agent_id, run_id)
    }

    /// Project an exact run result for CLI callers which already pinned the run.
    pub fn public_run_result(&self, run_id: &AgentId, value: Value) -> Result<Value> {
        result(&Store::open(&self.home)?.get(run_id)?, value)
    }

    /// Return paged execution history with stable agent ids and exact run ids.
    ///
    /// Existing pagination counts executions, including resumed runs; it is not
    /// silently regrouped or filtered, so no historical run disappears.
    pub async fn list_public(&self, query: crate::service::Query) -> Result<Value> {
        let mut value = self.list(query).await?;
        if let Some(items) = value["items"].as_array_mut() {
            for item in items {
                *item = view(item.take())?;
            }
        }
        Ok(value)
    }

    /// Continue the current run of a stable agent, preserving request-id replay.
    ///
    /// A replay resolves its original parent before checking terminal status,
    /// even if the lineage has since advanced. The existing resume admission
    /// verifies the request fingerprint, native identity, cleanup and unique
    /// child transaction; none of those guards is bypassed here. A conflicting
    /// request id or a pinned run from another lineage fails without a launch.
    #[allow(clippy::too_many_arguments)]
    pub async fn resume_public(
        &self,
        agent_id: &AgentId,
        run_id: Option<&AgentId>,
        task: String,
        timeout: Option<f64>,
        request_id: Option<String>,
        orchestrator: Option<crate::domain::OrchestratorRef>,
    ) -> Result<Value> {
        let parent = {
            let store = Store::open(&self.home)?;
            let selected = resolve(&store, agent_id, run_id)?;
            let mut replay_request = selected.request.clone();
            replay_request.request_id = request_id.clone();
            if orchestrator.is_some() {
                replay_request.orchestrator = orchestrator.clone();
            }
            match store.replay_request(&replay_request)? {
                Some(previous) => {
                    let parent_id = previous.parent_agent_id.ok_or(Error::Conflict)?;
                    if previous.root_agent_id != selected.root_agent_id
                        || run_id.is_some_and(|id| id != &parent_id)
                    {
                        return Err(Error::Conflict);
                    }
                    store.get(&parent_id)?
                }
                None => selected,
            }
        };
        admission(
            self.resume(&parent.id, task, timeout, request_id, orchestrator)
                .await?,
        )
    }
}
