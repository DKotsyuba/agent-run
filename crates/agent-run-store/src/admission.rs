//! Atomic SQLite admission for durable agent ownership.
//!
//! This module is the only writer that reserves an active slot, establishes
//! request-id idempotency, persists the frozen launch identity, and appends the
//! `created` and `start_accepted` events. Its `IMMEDIATE` transaction makes
//! those facts visible together or not at all.

use crate::{lineage, tx_event, Record, Store, ACTIVE_SQL};
use agent_run_config::config::Config;
use agent_run_domain::{
    domain::{now, AgentId, StartRequest, Status},
    Error, Result,
};
use agent_run_platform::process;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde_json::{json, Value};

/// Return the recorded request-id replay in its canonical or exact legacy namespace.
///
/// This read does not alter historical rows or load mutable configuration.
pub fn replay_request(store: &Store, request: &StartRequest) -> Result<Option<Record>> {
    let Some(request_id) = request.request_id.as_deref() else {
        return Ok(None);
    };
    let Some(reference) = &request.orchestrator else {
        return Ok(store
            .conn
            .query_row(
                "SELECT a.* FROM agents a WHERE a.request_id=? AND (a.orchestrator_session_id IS NULL OR json_extract(a.request_json,'$.orchestrator') IS NULL) ORDER BY a.created_at LIMIT 1",
                [request_id],
                Record::read,
            )
            .optional()?);
    };
    let transport = reference.canonical_transport()?;
    let legacy = match transport {
        "codex_queue" => "codex",
        "claude_uds" => "claude",
        _ => unreachable!(),
    };
    Ok(store
        .conn
        .query_row(
            "SELECT a.* FROM agents a LEFT JOIN orchestrator_sessions o ON o.id=a.orchestrator_session_id \
             WHERE a.request_id=? AND o.transport IN (?,?) AND o.external_session_id=? \
             ORDER BY (o.transport=?) DESC,a.created_at LIMIT 1",
            params![request_id, transport, legacy, reference.external_session_id, transport],
            Record::read,
        )
        .optional()?)
}

/// Atomically create one `starting` agent or return its exact request replay.
///
/// The validated effective request, non-secret identity snapshot, parent
/// lineage, capacity reservation, startup owner proof, and initial events are
/// committed in one SQLite `BEGIN IMMEDIATE` transaction. A conflicting replay
/// or exhausted global/runtime cap changes no rows. The compatibility wrapper
/// retains Python's `pending:materialization` marker until preparation seals a
/// snapshot; the service uses [`admit_with_config_revision`] for a canonical
/// role-plan hash.
pub fn admit(
    store: &mut Store,
    request: &StartRequest,
    config: &Config,
    identity: &Value,
    parent: Option<&Record>,
) -> Result<(AgentId, bool)> {
    admit_with_config_revision(
        store,
        request,
        config,
        "pending:materialization",
        identity,
        parent,
    )
}

/// Atomically create an admission using the already-frozen configuration revision.
///
/// `config_revision` is the canonical role-plan hash for revisioned profiles,
/// or Python's legacy pending-materialization marker. It is validated before
/// the transaction begins and never affects request replay equality.
pub fn admit_with_config_revision(
    store: &mut Store,
    request: &StartRequest,
    config: &Config,
    config_revision: &str,
    identity: &Value,
    parent: Option<&Record>,
) -> Result<(AgentId, bool)> {
    agent_run_domain::domain::nonblank("config_revision", config_revision)?;
    let mut checked = request.clone();
    checked.validate()?;
    let tx = store
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let accepted_at = now();

    // Replay precedes session upsert and capacity: Python deliberately makes a
    // repeated accepted request insensitive to later mutable config edits.
    let session = replay_session(&tx, &checked)?;
    if checked.orchestrator.is_none() || session.is_some() {
        if let Some(found) = replay_in_transaction(&tx, &checked, session.as_deref())? {
            ensure_same_replay(&found, &checked, identity, parent)?;
            tx.commit()?;
            return Ok((found.id, false));
        }
    }

    let global: i64 = tx.query_row(
        &format!("SELECT COUNT(*) FROM agents WHERE status IN {ACTIVE_SQL}"),
        [],
        |row| row.get(0),
    )?;
    let runtime_count: i64 = tx.query_row(
        &format!("SELECT COUNT(*) FROM agents WHERE status IN {ACTIVE_SQL} AND runtime=?"),
        [&checked.runtime],
        |row| row.get(0),
    )?;
    let runtime = config.runtime(&checked.runtime)?;
    if global >= config.core.max_active_agents as i64
        || runtime
            .max_active_agents
            .is_some_and(|limit| runtime_count >= limit as i64)
    {
        return Err(Error::Capacity);
    }

    let lineage = parent
        .map(|record| lineage::resume_parent(&tx, &record.id))
        .transpose()?;
    // A runtime home with a still-prepared storage layout must not admit a
    // new continuation while its physical placement is being changed; this
    // runs inside the same transaction as the insert, so the two can never
    // race. The frozen parent identity, never the request path, names the home.
    if let Some(identity) = parent.and_then(|record| record.identity.as_ref()) {
        crate::runtime_storage::refuse_prepared_resume_home(&tx, identity)?;
    }
    let session = upsert_session(&tx, &checked, accepted_at)?;
    let id = AgentId::new();
    let summary = checked
        .task
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(160)
        .collect::<String>();
    let root = lineage
        .as_ref()
        .map(|lineage| lineage.root_agent_id.as_str())
        .unwrap_or(id.as_str());
    let sequence = lineage
        .as_ref()
        .map(|lineage| lineage.sequence)
        .unwrap_or(1);
    let resume_session = lineage
        .as_ref()
        .map(|lineage| lineage.runtime_session_id.as_str());
    let inserted = tx.execute("INSERT INTO agents(id,request_id,orchestrator_session_id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,parent_agent_id,root_agent_id,sequence,resume_of_runtime_session_id,identity_json,display_name) VALUES(?,?,?,?,?,?,?,?,?,?,'starting',?,?,?,?,?,?,?,?,?)", params![id.as_str(), checked.request_id, session, checked.runtime, checked.model, checked.profile, checked.task, summary, checked.workdir.to_string_lossy(), serde_json::to_string(&checked)?, accepted_at, checked.timeout_seconds.unwrap_or_else(|| config.core.effective_default_timeout_seconds()), config_revision, parent.map(|record| record.id.as_str()), root, sequence, resume_session, serde_json::to_string(&identity)?, checked.display_name]);
    if let Err(error) = inserted {
        let latest: Option<String> = tx
            .query_row(
                "SELECT id FROM agents WHERE parent_agent_id=? ORDER BY sequence DESC LIMIT 1",
                [parent.map(|record| record.id.as_str())],
                |row| row.get(0),
            )
            .optional()?;
        if latest.is_some() {
            return Err(Error::Conflict);
        }
        return Err(error.into());
    }
    if let Ok(owner) = process::inspect(std::process::id() as i32) {
        tx.execute(
            "UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=? WHERE id=?",
            params![serde_json::to_string(&owner)?, owner.birth, id.as_str()],
        )?;
    }
    tx_event(&tx, &id, "created", None, Some(Status::Created), &json!({}))?;
    tx_event(
        &tx,
        &id,
        "start_accepted",
        Some(Status::Created),
        Some(Status::Starting),
        &json!({"durable_admission": true}),
    )?;
    tx.commit()?;
    Ok((id, true))
}

/// Resolves the canonical-first existing session family without touching it.
pub(crate) fn replay_session(
    tx: &rusqlite::Transaction<'_>,
    request: &StartRequest,
) -> Result<Option<String>> {
    let Some(reference) = &request.orchestrator else {
        return Ok(None);
    };
    Ok(crate::session_ids_for_reference(tx, reference)?
        .into_iter()
        .next())
}

/// Finds a request id under the exact session family already present in the transaction.
pub(crate) fn replay_in_transaction(
    tx: &rusqlite::Transaction<'_>,
    request: &StartRequest,
    session: Option<&str>,
) -> Result<Option<Record>> {
    let Some(request_id) = request.request_id.as_deref() else {
        return Ok(None);
    };
    let Some(reference) = &request.orchestrator else {
        return Ok(tx
            .query_row(
                "SELECT * FROM agents WHERE request_id=? AND (orchestrator_session_id IS NULL OR json_extract(request_json,'$.orchestrator') IS NULL) ORDER BY created_at LIMIT 1",
                params![request_id],
                Record::read,
            )
            .optional()?);
    };
    if session.is_none() {
        return Ok(None);
    }
    let canonical = reference.canonical_transport()?;
    let legacy = match canonical {
        "codex_queue" => "codex",
        "claude_uds" => "claude",
        _ => unreachable!(),
    };
    Ok(tx
        .query_row(
            "SELECT a.* FROM agents a JOIN orchestrator_sessions o ON o.id=a.orchestrator_session_id \
             WHERE a.request_id=? AND o.external_session_id=? AND o.transport IN (?,?) \
             ORDER BY (o.transport=?) DESC,a.created_at LIMIT 1",
            params![request_id, reference.external_session_id, canonical, legacy, canonical],
            Record::read,
        )
        .optional()?)
}

/// Compares every frozen request field while ignoring only known transport spelling.
/// Unknown names or differing session ids do not normalize as equivalent.
fn replay_request_equivalent(found: &StartRequest, request: &StartRequest) -> bool {
    let mut found = found.clone();
    let mut request = request.clone();
    match (&mut found.orchestrator, &mut request.orchestrator) {
        (Some(found_ref), Some(request_ref)) => {
            let Ok(found_transport) = found_ref.canonical_transport() else {
                return false;
            };
            let Ok(request_transport) = request_ref.canonical_transport() else {
                return false;
            };
            if found_transport != request_transport {
                return false;
            }
            found_ref.transport = found_transport.to_owned();
            request_ref.transport = request_transport.to_owned();
        }
        (None, None) => {}
        _ => return false,
    }
    found == request
}

/// Reject a request id if its immutable request intent or resume parent differs.
///
/// Present request hashes must match. Service replay checks historical transport
/// hash variants before admission; a spelling difference never waives a mismatch
/// here. Without hashes on both sides, the requests must equal apart from known
/// transport spelling.
fn ensure_same_replay(
    found: &Record,
    request: &StartRequest,
    identity: &Value,
    parent: Option<&Record>,
) -> Result<()> {
    let hash = |value: &Value| {
        value
            .get("replay_request_sha256")
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    let same_request = match (hash(identity), found.identity.as_ref().and_then(hash)) {
        (Some(current), Some(previous)) => current == previous,
        _ => replay_request_equivalent(&found.request, request),
    };
    if !same_request || found.parent_agent_id.as_ref() != parent.map(|record| &record.id) {
        return Err(Error::Conflict);
    }
    Ok(())
}

/// Reuses the shared identity lookup when a new admission has an orchestrator.
/// Existing exact legacy aliases keep their row id; fresh rows use canonical names.
pub(crate) fn upsert_session(
    tx: &rusqlite::Transaction<'_>,
    request: &StartRequest,
    accepted_at: f64,
) -> Result<Option<String>> {
    request
        .orchestrator
        .as_ref()
        .map(|reference| crate::session_for_reference(tx, reference, accepted_at))
        .transpose()
}

impl Store {
    /// Return a prior request-id admission without loading mutable configuration.
    pub fn replay_request(&self, request: &StartRequest) -> Result<Option<Record>> {
        replay_request(self, request)
    }

    /// Return the already-admitted child of exactly `parent` for this resume intent.
    ///
    /// Reads only durable rows, never configuration, so a retry of an old
    /// parent still finds its child after later continuations advanced the
    /// lineage. A recorded request id with a different parent or request
    /// (other than known transport spelling) is not a replay: `None`, leaving
    /// the ordinary admission to refuse it.
    pub fn replay_resume_child(
        &self,
        request: &StartRequest,
        parent: &Record,
    ) -> Result<Option<Record>> {
        Ok(replay_request(self, request)?.filter(|found| {
            found.parent_agent_id.as_ref() == Some(&parent.id)
                && replay_request_equivalent(&found.request, request)
        }))
    }

    /// Atomically reserve capacity and persist one durable admission or replay.
    pub fn admit(
        &mut self,
        request: &StartRequest,
        config: &Config,
        identity: &Value,
        parent: Option<&Record>,
    ) -> Result<(AgentId, bool)> {
        admit(self, request, config, identity, parent)
    }

    /// Atomically admit with the caller's frozen configuration revision.
    pub fn admit_with_config_revision(
        &mut self,
        request: &StartRequest,
        config: &Config,
        config_revision: &str,
        identity: &Value,
        parent: Option<&Record>,
    ) -> Result<(AgentId, bool)> {
        admit_with_config_revision(self, request, config, config_revision, identity, parent)
    }
}
