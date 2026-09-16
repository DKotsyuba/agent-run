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

/// Return the recorded request-id replay in its orchestrator namespace.
///
/// This read deliberately has no mutable-config dependency, letting the
/// service return an already admitted request after configuration was edited or
/// made invalid. Concurrent first submissions are resolved again inside
/// [`Store::admit`]'s immediate transaction.
pub fn replay_request(store: &Store, request: &StartRequest) -> Result<Option<Record>> {
    let Some(request_id) = request.request_id.as_deref() else {
        return Ok(None);
    };
    let transport = request
        .orchestrator
        .as_ref()
        .map(|item| item.transport.as_str());
    let session = request
        .orchestrator
        .as_ref()
        .map(|item| item.external_session_id.as_str());
    let sql = if request.orchestrator.is_none() {
        "SELECT a.* FROM agents a WHERE a.request_id=? AND (a.orchestrator_session_id IS NULL OR json_extract(a.request_json,'$.orchestrator') IS NULL) ORDER BY a.created_at LIMIT 1"
    } else {
        "SELECT a.* FROM agents a LEFT JOIN orchestrator_sessions o ON o.id=a.orchestrator_session_id WHERE a.request_id=? AND o.transport=? AND o.external_session_id=? ORDER BY a.created_at LIMIT 1"
    };
    let row = if request.orchestrator.is_none() {
        store
            .conn
            .query_row(sql, params![request_id], Record::read)
            .optional()?
    } else {
        store
            .conn
            .query_row(sql, params![request_id, transport, session], Record::read)
            .optional()?
    };
    Ok(row)
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
    let inserted = tx.execute("INSERT INTO agents(id,request_id,orchestrator_session_id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,parent_agent_id,root_agent_id,sequence,resume_of_runtime_session_id,identity_json) VALUES(?,?,?,?,?,?,?,?,?,?,'starting',?,?,?,?,?,?,?,?)", params![id.as_str(), checked.request_id, session, checked.runtime, checked.model, checked.profile, checked.task, summary, checked.workdir.to_string_lossy(), serde_json::to_string(&checked)?, accepted_at, checked.timeout_seconds.unwrap_or(config.core.default_timeout_seconds), config_revision, parent.map(|record| record.id.as_str()), root, sequence, resume_session, serde_json::to_string(identity)?]);
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

/// Look up replay's namespace without creating or updating its session row.
fn replay_session(
    tx: &rusqlite::Transaction<'_>,
    request: &StartRequest,
) -> Result<Option<String>> {
    let Some(orchestrator) = &request.orchestrator else {
        return Ok(None);
    };
    Ok(tx
        .query_row(
            "SELECT id FROM orchestrator_sessions WHERE transport=? AND external_session_id=?",
            params![orchestrator.transport, orchestrator.external_session_id],
            |row| row.get(0),
        )
        .optional()?)
}

/// Find an existing request id scoped by the already-resolved session id.
fn replay_in_transaction(
    tx: &rusqlite::Transaction<'_>,
    request: &StartRequest,
    session: Option<&str>,
) -> Result<Option<Record>> {
    let Some(request_id) = request.request_id.as_deref() else {
        return Ok(None);
    };
    let row = if session.is_none() {
        tx.query_row(
            "SELECT * FROM agents WHERE request_id=? AND (orchestrator_session_id IS NULL OR json_extract(request_json,'$.orchestrator') IS NULL) ORDER BY created_at LIMIT 1",
            params![request_id],
            Record::read,
        ).optional()?
    } else {
        tx.query_row(
            "SELECT * FROM agents WHERE request_id=? AND orchestrator_session_id IS ? ORDER BY created_at LIMIT 1",
            params![request_id, session],
            Record::read,
        ).optional()?
    };
    Ok(row)
}

/// Reject a request id if its immutable request hash or resume parent differs.
fn ensure_same_replay(
    found: &Record,
    request: &StartRequest,
    identity: &Value,
    parent: Option<&Record>,
) -> Result<()> {
    let same_request = identity
        .get("replay_request_sha256")
        .and_then(Value::as_str)
        .zip(
            found
                .identity
                .as_ref()
                .and_then(|value| value.get("replay_request_sha256"))
                .and_then(Value::as_str),
        )
        .map(|(current, previous)| current == previous)
        .unwrap_or(found.request == *request);
    if !same_request || found.parent_agent_id.as_ref() != parent.map(|record| &record.id) {
        return Err(Error::Conflict);
    }
    Ok(())
}

/// Upsert the session only for a new admission, after replay and capacity pass.
fn upsert_session(
    tx: &rusqlite::Transaction<'_>,
    request: &StartRequest,
    accepted_at: f64,
) -> Result<Option<String>> {
    let Some(orchestrator) = &request.orchestrator else {
        return Ok(None);
    };
    let id = format!("os-{}", uuid::Uuid::new_v4().simple());
    tx.execute("INSERT INTO orchestrator_sessions(id,transport,external_session_id,external_turn_id,created_at,last_seen_at) VALUES(?,?,?,?,?,?) ON CONFLICT(transport,external_session_id) DO UPDATE SET last_seen_at=excluded.last_seen_at,external_turn_id=excluded.external_turn_id", params![id, orchestrator.transport, orchestrator.external_session_id, orchestrator.external_turn_id, accepted_at, accepted_at])?;
    Ok(Some(tx.query_row(
        "SELECT id FROM orchestrator_sessions WHERE transport=? AND external_session_id=?",
        params![orchestrator.transport, orchestrator.external_session_id],
        |row| row.get(0),
    )?))
}

impl Store {
    /// Return a prior request-id admission without loading mutable configuration.
    pub fn replay_request(&self, request: &StartRequest) -> Result<Option<Record>> {
        replay_request(self, request)
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
