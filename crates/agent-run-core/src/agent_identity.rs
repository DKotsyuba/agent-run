//! Stable public agent identities over immutable execution records.
//!
//! Storage, supervisors and delivery leases continue to address exact run rows.
//! Public callers use only the lineage root; legacy execution selectors remain accepted.

use crate::{Error, Result, domain::AgentId, error::invalid, state::Record, state::Store};
use rusqlite::OptionalExtension;
use serde_json::{Value, json};

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

/// Project one execution as the stable public agent without changing user content.
///
/// Only product-owned envelope, agent and delivery metadata are normalized.
/// Answers, transcript text, policy and arbitrary tool content remain untouched.
pub fn result(row: &Record, mut value: Value) -> Result<Value> {
    identify(&mut value, &row.root_agent_id)?;
    Ok(value)
}

/// Convert an internal execution view into one stable public agent view.
pub fn view(mut value: Value) -> Result<Value> {
    let root: AgentId = serde_json::from_value(value["root_agent_id"].clone())?;
    identify(&mut value, &root)?;
    Ok(value)
}

/// Project an admitted execution after its supervisor handoff.
///
/// The stable ID is the only public agent identifier. Its existing sequence
/// counter is retained as a machine receipt for delayed hooks and CLI wait;
/// neither transport must guess the latest execution when consuming a receipt.
pub fn admission(mut value: Value) -> Result<Value> {
    let sequence = value["agent"]["sequence"].clone();
    value["agent"] = view(value["agent"].take())?;
    let root = serde_json::from_value(value["agent"]["agent_id"].clone())?;
    identify(&mut value, &root)?;
    value["sequence"] = sequence;
    Ok(value)
}

/// Remove execution identities only from known product metadata objects.
fn identify(value: &mut Value, root: &AgentId) -> Result<()> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| invalid("agent result must be an object"))?;
    object.insert("agent_id".into(), json!(root));
    for key in [
        "run_id",
        "parent_run_id",
        "parent_agent_id",
        "root_agent_id",
        "attempt_id",
    ] {
        object.remove(key);
    }
    for key in ["agent", "delivery"] {
        if let Some(nested @ Value::Object(_)) = object.get_mut(key) {
            identify(nested, root)?;
        }
    }
    Ok(())
}

/// Resolve a transport receipt to its exact execution, never to a moving tip.
///
/// The positive sequence is an internal counter, not an orchestrator selector.
/// Missing or ambiguous receipts fail closed, including after history retention.
pub fn resolve_sequence(store: &Store, agent_id: &AgentId, sequence: u32) -> Result<Record> {
    if sequence == 0 {
        return Err(invalid("execution receipt sequence must be positive"));
    }
    let root = match store.get(agent_id) {
        Ok(row) => row.root_agent_id,
        Err(Error::NotFound(_)) => agent_id.clone(),
        Err(error) => return Err(error),
    };
    let mut statement = store.conn.prepare(
        "SELECT id FROM agents WHERE (root_agent_id=?1 OR id=?1) AND sequence=?2 LIMIT 2",
    )?;
    let ids = statement
        .query_map(rusqlite::params![root.as_str(), sequence], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    match ids.as_slice() {
        [id] => store.get(&id.parse()?),
        [] => Err(Error::NotFound(root.to_string())),
        _ => Err(invalid("execution receipt is ambiguous")),
    }
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

    /// Return one latest view per logical agent with exact logical pagination.
    pub async fn list_public(&self, query: crate::service::Query) -> Result<Value> {
        let mut value = self.list_selected(query, true).await?;
        if let Some(items) = value["items"].as_array_mut() {
            for item in items {
                *item = view(item.take())?;
            }
        }
        Ok(value)
    }

    /// Read retained lineage messages using the same global cursor across resumes.
    pub fn transcript_public(&self, id: &AgentId, cursor: i64, limit: usize) -> Result<Value> {
        let store = Store::open(&self.home)?;
        let row = resolve(&store, id, None)?;
        result(&row, store.transcript_lineage(&row.id, cursor, limit)?)
    }

    /// Reads validated raw or block history for a stable selection. An explicit
    /// run pins one execution; omission reads its retained lineage. Resolves once,
    /// keeps cursors global and removes internal execution IDs from the envelope.
    /// No process is launched; validation, store and selection errors propagate.
    pub fn transcript_with_options(
        &self,
        id: &AgentId,
        run_id: Option<&AgentId>,
        query: &agent_run_domain::transcript::TranscriptQuery,
    ) -> Result<Value> {
        query.validate()?;
        let store = Store::open(&self.home)?;
        let row = resolve(&store, id, run_id)?;
        result(
            &row,
            store.transcript_query(&row.id, query, run_id.is_none())?,
        )
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
        request_id: Option<String>,
        display_name: Option<String>,
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
            self.resume(&parent.id, task, request_id, display_name, orchestrator)
                .await?,
        )
    }
}
