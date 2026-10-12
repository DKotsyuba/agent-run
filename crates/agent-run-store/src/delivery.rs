//! Durable completion-delivery rows and immutable queue-attempt evidence.
//!
//! These helpers keep terminal notices, late orchestrator binding, and evidence
//! verdicts within SQLite transactions owned by the calling [`Store`].

use crate::Store;
use agent_run_domain::{
    Result,
    domain::{AgentId, now},
    error::invalid,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Map, Value, json};

/// Maximum age of an unbound completion notice before it is permanently expired.
pub const BINDING_WINDOW_SECONDS: f64 = 3600.0;
/// Maximum UTF-8 bytes retained in one persisted stdout or stderr evidence tail.
pub const MAX_EVIDENCE_TAIL_BYTES: usize = 4096;
/// Maximum UTF-8 bytes retained in one complete evidence JSON document.
pub const MAX_EVIDENCE_BYTES: usize = 16 * 1024;

/// Inserts a durable completion notice for a terminal event when one is needed.
///
/// Successful runs in a current pool seat omit individual notices;
/// the pool's common completion notice represents that success. Failures and
/// other terminal outcomes still create individual notices. A run with an
/// orchestrator session starts `pending`; without one, no delivery row can be
/// activated. The caller owns the transaction, keeping any row atomic with its
/// terminal event. Repeated calls for the same terminal event reuse its notice,
/// including failed or expired rows. Returns the notification id when present, otherwise
/// `None`.
pub(crate) fn insert_terminal_notice(
    tx: &rusqlite::Connection,
    id: &AgentId,
    session: Option<&str>,
    terminal_event_seq: i64,
    at: f64,
) -> Result<Option<String>> {
    let suppress_member_success: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM agents a JOIN pool_members m ON m.agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id) JOIN pools p ON p.id=m.pool_id WHERE a.id=? AND a.status='succeeded' AND m.replaced_by IS NULL)",
        [id.as_str()],
        |row| row.get(0),
    )?;
    if suppress_member_success {
        return Ok(None);
    }
    let Some(session) = session else {
        return Ok(None);
    };
    let previous: Option<String> = tx
        .query_row(
            "SELECT id FROM deliveries WHERE agent_id=? AND terminal_event_seq=? LIMIT 1",
            params![id.as_str(), terminal_event_seq],
            |row| row.get(0),
        )
        .optional()?;
    if previous.is_some() {
        return Ok(previous);
    }
    let notification_id = format!("ntf_{}", uuid::Uuid::new_v4().simple());
    tx.execute(
        "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) VALUES(?,?,?,?,?,?)",
        params![notification_id, id.as_str(), session, terminal_event_seq, "pending", at],
    )?;
    Ok(Some(notification_id))
}

/// Repairs a completion omitted before binding, atomically with the binding transaction.
/// Only a durable terminal status event can create a notice. Existing notices are reused
/// and successful current pool members remain suppressed; no historical row is resurrected.
pub(crate) fn ensure_bound_terminal_notice(
    tx: &rusqlite::Connection,
    id: &AgentId,
    session: &str,
    at: f64,
) -> Result<()> {
    let event: Option<i64> = tx.query_row(
        "SELECT MAX(e.seq) FROM events e JOIN agents a ON a.id=e.agent_id \
         WHERE a.id=? AND a.status IN ('succeeded','failed','timed_out','cancelled','lost') \
         AND e.kind='status' AND e.to_status=a.status",
        [id.as_str()],
        |row| row.get(0),
    )?;
    if let Some(event) = event {
        insert_terminal_notice(tx, id, Some(session), event, at)?;
    }
    Ok(())
}

/// Redacts one diagnostic suffix, retaining at most [`MAX_EVIDENCE_TAIL_BYTES`] UTF-8 bytes.
fn redact_tail(text: &str) -> String {
    let mut safe = text.to_owned();
    for marker in [
        "authorization",
        "api_key",
        "api-key",
        "token",
        "secret",
        "password",
        "bearer ",
    ] {
        let mut cursor = 0;
        while let Some(found) = safe[cursor..].to_ascii_lowercase().find(marker) {
            let start = cursor + found;
            let end = safe[start..]
                .find(['\n', '\r'])
                .map(|offset| start + offset)
                .unwrap_or(safe.len());
            safe.replace_range(start..end, "[redacted]");
            cursor = start + "[redacted]".len();
        }
    }
    while safe.len() > MAX_EVIDENCE_TAIL_BYTES {
        let mut start = safe.len() - MAX_EVIDENCE_TAIL_BYTES;
        while !safe.is_char_boundary(start) {
            start += 1;
        }
        safe = safe[start..].to_owned();
    }
    safe
}

/// Validates and sanitizes Python-compatible delivery evidence before persistence.
fn safe_evidence(raw: &Value) -> Result<Value> {
    let expected = [
        "classifier",
        "executable",
        "argv_shape",
        "duration_ms",
        "returncode",
        "spawn_errno",
        "error_class",
        "stdout_tail",
        "stderr_tail",
        "stdout_bytes",
        "stderr_bytes",
        "stdout_truncated",
        "stderr_truncated",
        "message_id_present",
    ];
    let Some(object) = raw.as_object() else {
        return Err(invalid("invalid delivery attempt evidence"));
    };
    if object.len() != expected.len() || expected.iter().any(|key| !object.contains_key(*key)) {
        return Err(invalid("invalid delivery attempt evidence"));
    }
    let nonblank = |key: &str| {
        object
            .get(key)
            .and_then(Value::as_str)
            .filter(|v| !v.trim().is_empty())
    };
    if nonblank("classifier").is_none()
        || nonblank("executable").is_none()
        || object.get("duration_ms").and_then(Value::as_u64).is_none()
        || object.get("stdout_bytes").and_then(Value::as_u64).is_none()
        || object.get("stderr_bytes").and_then(Value::as_u64).is_none()
        || !object
            .get("stdout_truncated")
            .is_some_and(Value::is_boolean)
        || !object
            .get("stderr_truncated")
            .is_some_and(Value::is_boolean)
        || !object
            .get("message_id_present")
            .is_some_and(Value::is_boolean)
    {
        return Err(invalid("invalid delivery attempt evidence"));
    }
    let Some(argv) = object.get("argv_shape").and_then(Value::as_array) else {
        return Err(invalid("invalid delivery attempt evidence"));
    };
    if argv.is_empty()
        || argv.iter().any(|part| {
            part.as_str().is_none_or(|part| {
                part.trim().is_empty()
                    || part.len() > 64
                    || !part.bytes().all(|byte| {
                        byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || byte == b'_'
                            || byte == b'-'
                    })
            })
        })
    {
        return Err(invalid("invalid delivery attempt evidence"));
    }
    for key in ["returncode", "spawn_errno"] {
        if !object
            .get(key)
            .is_some_and(|value| value.is_null() || value.as_i64().is_some())
        {
            return Err(invalid("invalid delivery attempt evidence"));
        }
    }
    if object["returncode"].is_number() && object["spawn_errno"].is_number() {
        return Err(invalid("invalid delivery attempt evidence"));
    }
    if !object.get("error_class").is_some_and(|value| {
        value.is_null()
            || value
                .as_str()
                .is_some_and(|text| !text.trim().is_empty() && text.len() <= 128)
    }) {
        return Err(invalid("invalid delivery attempt evidence"));
    }
    let Some(stdout) = object.get("stdout_tail").and_then(Value::as_str) else {
        return Err(invalid("invalid delivery attempt evidence"));
    };
    let Some(stderr) = object.get("stderr_tail").and_then(Value::as_str) else {
        return Err(invalid("invalid delivery attempt evidence"));
    };
    let stdout = redact_tail(stdout);
    let stderr = redact_tail(stderr);
    let mut value: Map<String, Value> = object.clone();
    value.insert("stdout_tail".into(), Value::String(stdout.clone()));
    value.insert("stderr_tail".into(), Value::String(stderr.clone()));
    value.insert(
        "stdout_truncated".into(),
        Value::Bool(
            object["stdout_truncated"].as_bool().unwrap_or(false)
                || stdout.len() < object["stdout_tail"].as_str().unwrap_or_default().len(),
        ),
    );
    value.insert(
        "stderr_truncated".into(),
        Value::Bool(
            object["stderr_truncated"].as_bool().unwrap_or(false)
                || stderr.len() < object["stderr_tail"].as_str().unwrap_or_default().len(),
        ),
    );
    let value = Value::Object(value);
    if serde_json::to_vec(&value)?.len() > MAX_EVIDENCE_BYTES {
        return Err(invalid("delivery attempt evidence exceeds 16384 bytes"));
    }
    Ok(value)
}

/// Validates a nonblank delivery operation string and returns it unchanged.
fn required_text(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(invalid(format!("{name} must be nonblank")));
    }
    Ok(())
}

/// Validates one finite nonnegative timestamp used by deterministic callers.
fn checked_time(name: &str, value: f64) -> Result<()> {
    if !value.is_finite() || value < 0.0 {
        return Err(invalid(format!("{name} must be finite and nonnegative")));
    }
    Ok(())
}

/// Claims the oldest due delivery or reclaims one expired sending lease.
pub(crate) fn claim(
    store: &mut Store,
    owner: &str,
    at: f64,
    lease_seconds: f64,
) -> Result<Option<Value>> {
    required_text("lease owner", owner)?;
    checked_time("claim time", at)?;
    if !lease_seconds.is_finite() || lease_seconds <= 0.0 {
        return Err(invalid("lease_seconds must be positive and finite"));
    }
    let tx = crate::writer::WriteTx::begin(&store.conn)?;
    let id: Option<String> = tx
        .query_row(
            "SELECT id FROM deliveries WHERE ((state IN ('pending','retry_wait') AND COALESCE(next_attempt_at,0)<=?) OR (state='sending' AND lease_until<=?)) ORDER BY COALESCE(next_attempt_at,lease_until,0),id LIMIT 1",
            params![at, at],
            |row| row.get(0),
        )
        .optional()?;
    let Some(id) = id else {
        tx.commit()?;
        return Ok(None);
    };
    let updated = tx.execute(
        "UPDATE deliveries SET state='sending',attempts=attempts+1,lease_owner=?,lease_until=?,next_attempt_at=NULL WHERE id=? AND ((state IN ('pending','retry_wait') AND COALESCE(next_attempt_at,0)<=?) OR (state='sending' AND lease_until<=?))",
        params![owner, at + lease_seconds, id, at, at],
    )?;
    if updated != 1 {
        tx.commit()?;
        return Ok(None);
    }
    let row = tx.query_row(
        "SELECT d.id,d.attempts,d.orchestrator_session_id,d.agent_id,a.status AS agent_status,s.transport,s.external_session_id,s.external_turn_id FROM deliveries d JOIN agents a ON a.id=d.agent_id JOIN orchestrator_sessions s ON s.id=d.orchestrator_session_id WHERE d.id=?",
        [&id],
        |row| {
            Ok(json!({
                "id": row.get::<_, String>(0)?,
                "attempts": row.get::<_, u32>(1)?,
                "orchestrator_session_id": row.get::<_, Option<String>>(2)?,
                "agent_id": row.get::<_, String>(3)?,
                "agent_status": row.get::<_, String>(4)?,
                "transport": row.get::<_, String>(5)?,
                "external_session_id": row.get::<_, String>(6)?,
                "external_turn_id": row.get::<_, Option<String>>(7)?,
            }))
        },
    )?;
    tx.commit()?;
    Ok(Some(row))
}

/// Persists evidence for the caller's live attempt and returns its attempt number.
fn owned_attempt(
    tx: &rusqlite::Connection,
    delivery_id: &str,
    owner: &str,
    at: f64,
    evidence: Option<&Value>,
) -> Result<u32> {
    let attempt: Option<u32> = tx
        .query_row(
            "SELECT attempts FROM deliveries WHERE id=? AND state='sending' AND lease_owner=? AND lease_until>?",
            params![delivery_id, owner, at],
            |row| row.get(0),
        )
        .optional()?;
    let Some(attempt) = attempt else {
        return Err(invalid("delivery lease is not owned by caller"));
    };
    if let Some(evidence) = evidence {
        let evidence = safe_evidence(evidence)?;
        tx.execute(
            "INSERT INTO delivery_attempt_evidence(delivery_id,attempt,recorded_at,evidence_json) VALUES(?,?,?,?)",
            params![delivery_id, attempt, at, serde_json::to_string(&evidence)?],
        )?;
    }
    Ok(attempt)
}

/// Finishes a live delivery claim while preserving the strongest prior ambiguity flag.
#[allow(clippy::too_many_arguments)] // Mirrors the explicit Python delivery-finalization contract.
fn finish_claim(
    tx: &rusqlite::Connection,
    delivery_id: &str,
    owner: &str,
    state: &str,
    at: f64,
    error: Option<&str>,
    ambiguous: bool,
    next_attempt_at: Option<f64>,
    remote_message_id: Option<&str>,
) -> Result<()> {
    let changed = tx.execute(
        "UPDATE deliveries SET state=?,remote_message_id=COALESCE(?,remote_message_id),last_error=COALESCE(?,last_error),ambiguous_result=MAX(ambiguous_result,?),next_attempt_at=?,lease_owner=NULL,lease_until=NULL WHERE id=? AND state='sending' AND lease_owner=? AND lease_until>?",
        params![state, remote_message_id, error, ambiguous, next_attempt_at, delivery_id, owner, at],
    )?;
    if changed != 1 {
        return Err(invalid("delivery lease is not owned by caller"));
    }
    Ok(())
}

/// Best-effort incident capture after a legacy (autocommit) delivery
/// update. Inside the sole writer's transaction nothing is captured here:
/// the caller follows up with the separate incident capture operation, so
/// no capture work or discarded SQL failure runs in the writer transaction.
fn legacy_capture(store: &Store, agent: &str, at: f64) {
    if store.conn.is_autocommit() {
        let _ = crate::incidents::capture(&store.conn, agent, at);
    }
}

/// Completes one live delivery claim and optionally records immutable evidence.
pub(crate) fn complete(
    store: &mut Store,
    delivery_id: &str,
    owner: &str,
    at: f64,
    remote_message_id: Option<&str>,
    ambiguous: bool,
    evidence: Option<&Value>,
) -> Result<()> {
    required_text("delivery_id", delivery_id)?;
    required_text("lease owner", owner)?;
    checked_time("completion time", at)?;
    let tx = crate::writer::WriteTx::begin(&store.conn)?;
    owned_attempt(&tx, delivery_id, owner, at, evidence)?;
    let incident_agent: String = tx.query_row(
        "SELECT agent_id FROM deliveries WHERE id=?",
        [delivery_id],
        |row| row.get(0),
    )?;
    finish_claim(
        &tx,
        delivery_id,
        owner,
        "delivered",
        at,
        None,
        ambiguous,
        None,
        remote_message_id,
    )?;
    tx.commit()?;
    legacy_capture(store, &incident_agent, at);
    Ok(())
}

/// Marks one live delivery claim permanently failed and optionally records evidence.
pub(crate) fn fail(
    store: &mut Store,
    delivery_id: &str,
    owner: &str,
    error: &str,
    at: f64,
    ambiguous: bool,
    evidence: Option<&Value>,
) -> Result<()> {
    required_text("delivery_id", delivery_id)?;
    required_text("lease owner", owner)?;
    required_text("delivery error", error)?;
    checked_time("failure time", at)?;
    let tx = crate::writer::WriteTx::begin(&store.conn)?;
    owned_attempt(&tx, delivery_id, owner, at, evidence)?;
    let incident_agent: String = tx.query_row(
        "SELECT agent_id FROM deliveries WHERE id=?",
        [delivery_id],
        |row| row.get(0),
    )?;
    finish_claim(
        &tx,
        delivery_id,
        owner,
        "failed",
        at,
        Some(error),
        ambiguous,
        None,
        None,
    )?;
    tx.commit()?;
    legacy_capture(store, &incident_agent, at);
    Ok(())
}

/// Schedules an exponential-backoff retry for one live delivery claim.
#[allow(clippy::too_many_arguments)] // Mirrors the explicit Python retry contract.
pub(crate) fn retry(
    store: &mut Store,
    delivery_id: &str,
    owner: &str,
    error: &str,
    at: f64,
    ambiguous: bool,
    evidence: Option<&Value>,
    base_delay: f64,
    max_delay: f64,
) -> Result<f64> {
    required_text("delivery_id", delivery_id)?;
    required_text("lease owner", owner)?;
    required_text("delivery error", error)?;
    checked_time("retry time", at)?;
    if !base_delay.is_finite() || base_delay <= 0.0 || !max_delay.is_finite() || max_delay <= 0.0 {
        return Err(invalid("retry delays must be positive and finite"));
    }
    let tx = crate::writer::WriteTx::begin(&store.conn)?;
    let attempts = owned_attempt(&tx, delivery_id, owner, at, evidence)?;
    let exponent = attempts.saturating_sub(1).min(20);
    let delay = (base_delay * 2_f64.powi(exponent as i32)).min(max_delay);
    let next = at + delay;
    let incident_agent: String = tx.query_row(
        "SELECT agent_id FROM deliveries WHERE id=?",
        [delivery_id],
        |row| row.get(0),
    )?;
    finish_claim(
        &tx,
        delivery_id,
        owner,
        "retry_wait",
        at,
        Some(error),
        ambiguous,
        Some(next),
        None,
    )?;
    tx.commit()?;
    legacy_capture(store, &incident_agent, at);
    Ok(next)
}

/// Returns the latest validated evidence document for one delivery.
pub(crate) fn latest(store: &Store, delivery_id: &str) -> Result<Option<Value>> {
    latest_on(&store.conn, delivery_id)
}

/// Reads and sanitizes the latest delivery evidence through an existing database snapshot.
pub(crate) fn latest_on(conn: &Connection, delivery_id: &str) -> Result<Option<Value>> {
    required_text("delivery_id", delivery_id)?;
    let raw: Option<String> = conn
        .query_row(
            "SELECT evidence_json FROM delivery_attempt_evidence WHERE delivery_id=? ORDER BY attempt DESC LIMIT 1",
            [delivery_id],
            |row| row.get(0),
        )
        .optional()?;
    raw.map(|value| {
        let parsed: Value = serde_json::from_str(&value)
            .map_err(|_| invalid("invalid stored delivery attempt evidence"))?;
        safe_evidence(&parsed)
    })
    .transpose()
}

impl Store<'_> {
    /// Cancels a nonterminal completion delivery and reports whether it changed state.
    ///
    /// The caller supplies one nonblank durable delivery identifier. Delivered and
    /// already-cancelled rows remain immutable; all other delivery states lose any
    /// active lease and scheduled retry in the same immediate transaction. Unknown
    /// identifiers return `false` without creating state.
    pub fn cancel_delivery(&mut self, delivery_id: &str) -> Result<bool> {
        if delivery_id.trim().is_empty() {
            return Err(invalid("delivery_id must be nonblank"));
        }
        let transaction = crate::writer::WriteTx::begin(&self.conn)?;
        let changed = transaction.execute(
            "UPDATE deliveries SET state='cancelled',lease_owner=NULL,lease_until=NULL,next_attempt_at=NULL WHERE id=? AND state NOT IN ('delivered','cancelled')",
            [delivery_id],
        )? == 1;
        transaction.commit()?;
        Ok(changed)
    }

    /// Expires waiting completion notices older than the documented binding window.
    pub fn expire_unbound_deliveries(&mut self, at: f64) -> Result<Vec<String>> {
        self.expire_unbound_batch(at, None)
    }

    /// Expires at most `limit` (all when `None`) waiting completion notices
    /// older than the binding window, oldest first; the sole writer's row 57
    /// uses a bounded batch so its result stays within the receipt bound and
    /// repeated batches make progress.
    pub fn expire_unbound_batch(&mut self, at: f64, limit: Option<usize>) -> Result<Vec<String>> {
        if !at.is_finite() {
            return Err(invalid("expiry time must be finite"));
        }
        let limit = limit.map_or(-1, |limit| limit.min(i64::MAX as usize) as i64);
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let mut statement = tx.prepare(
            "SELECT d.id FROM deliveries d JOIN agents a ON a.id=d.agent_id JOIN events e ON e.seq=d.terminal_event_seq WHERE d.state='waiting_binding' AND (e.kind='pool_join_needs_action' OR (e.kind NOT IN ('worker_notification','pool_completed') AND a.status IN ('succeeded','failed','timed_out','cancelled','lost'))) AND e.at<=? ORDER BY e.at,d.id LIMIT ?",
        )?;
        let ids = statement
            .query_map(params![at - BINDING_WINDOW_SECONDS, limit], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(statement);
        for id in &ids {
            tx.execute("UPDATE deliveries SET state='expired',lease_owner=NULL,lease_until=NULL,next_attempt_at=NULL WHERE id=? AND state='waiting_binding'", [id])?;
        }
        tx.commit()?;
        Ok(ids)
    }

    /// Commits an owned delivery verdict with exactly one bounded immutable evidence row.
    pub fn persist_delivery_verdict(
        &mut self,
        delivery_id: &str,
        owner: &str,
        state: &str,
        evidence: &Value,
    ) -> Result<()> {
        if !["delivered", "retry_wait", "failed"].contains(&state) {
            return Err(invalid("invalid delivery verdict state"));
        }
        let evidence = safe_evidence(evidence)?;
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let at = now();
        let attempt: Option<(u32, String)> = tx.query_row(
            "SELECT d.attempts,o.transport FROM deliveries d JOIN orchestrator_sessions o ON o.id=d.orchestrator_session_id WHERE d.id=? AND d.state='sending' AND d.lease_owner=? AND d.lease_until>?",
            params![delivery_id, owner, at],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;
        let Some((attempt, transport)) = attempt else {
            return Err(invalid("delivery lease is not owned by caller"));
        };
        if transport != "codex_queue" {
            return Err(invalid(
                "delivery attempt evidence is only retained for codex_queue",
            ));
        }
        tx.execute(
            "INSERT INTO delivery_attempt_evidence(delivery_id,attempt,recorded_at,evidence_json) VALUES(?,?,?,?)",
            params![delivery_id, attempt, at, serde_json::to_string(&evidence)?],
        )?;
        tx.execute(
            "UPDATE deliveries SET state=?,lease_owner=NULL,lease_until=NULL,next_attempt_at=NULL WHERE id=? AND state='sending' AND lease_owner=? AND attempts=?",
            params![state, delivery_id, owner, attempt],
        )?;
        tx.commit()?;
        Ok(())
    }
}

/// Lease of one claimed delivery in seconds when the policy names none.
const DEFAULT_LEASE_SECONDS: f64 = 30.0;

/// Whether a stored completion field is displayable metadata.
fn bounded(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty() && value.chars().count() <= 128)
}

/// Payload facts of one claimed delivery, validated from durable rows.
fn claim_payload(tx: &Connection, delivery_id: &str, agent_id: &str) -> Result<Value> {
    let attention: Option<(String, String)> = tx
        .query_row(
            "SELECT json_extract(e.data_json,'$.pool_id'),json_extract(e.data_json,'$.notice') FROM deliveries d \
             JOIN events e ON e.seq=d.terminal_event_seq WHERE d.id=? AND e.kind='pool_join_needs_action'",
            [delivery_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let pool: Option<(String, String)> = tx
        .query_row(
            "SELECT p.id,json_extract(e.data_json,'$.notice') FROM pools p JOIN deliveries d ON d.id=p.completion_delivery_id \
             JOIN events e ON e.seq=d.terminal_event_seq WHERE p.completion_delivery_id=?",
            [delivery_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((kind, (pool_id, message))) = attention
        .map(|found| ("pool_attention", found))
        .or_else(|| pool.map(|found| ("pool", found)))
    {
        let notice = agent_run_domain::pool::PoolNotice {
            notification_id: delivery_id.to_owned(),
            pool_id: pool_id.parse()?,
            message,
        };
        notice.validate()?;
        return Ok(json!({
            "kind": kind,
            "notice": {
                "notification_id": notice.notification_id,
                "pool_id": notice.pool_id.as_str(),
                "message": notice.message,
            }
        }));
    }
    let root: String = tx.query_row(
        "SELECT CASE WHEN root_agent_id='' THEN id ELSE root_agent_id END FROM agents WHERE id=?",
        [agent_id],
        |row| row.get(0),
    )?;
    let worker: Option<(String, String)> = tx
        .query_row(
            "SELECT kind,message FROM worker_notifications WHERE delivery_id=?",
            [delivery_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((kind, message)) = worker {
        let kind = match kind.as_str() {
            "notice" => agent_run_domain::worker::WorkerMessageKind::Notice,
            "risk" => agent_run_domain::worker::WorkerMessageKind::Risk,
            "question" => agent_run_domain::worker::WorkerMessageKind::Question,
            "blocker" => agent_run_domain::worker::WorkerMessageKind::Blocker,
            _ => return Err(invalid("invalid stored worker kind")),
        };
        let entry = tx
            .query_row(
                "SELECT seq,author_kind,author_agent_id,author_name,author_role,direction,kind,\
                 severity,proposal_seq,roster_revision,decision,body FROM pool_entries WHERE delivery_id=?",
                [delivery_id],
                crate::pool_log::entry_view,
            )
            .optional()?
            .map(|entry| agent_run_domain::pool::render_entry(&entry))
            .transpose()?;
        let notice = agent_run_domain::worker::WorkerNotice {
            notification_id: delivery_id.to_owned(),
            agent_id: root.parse()?,
            run_id: agent_id.parse()?,
            kind,
            message,
        };
        notice.validate()?;
        return Ok(json!({"kind": "worker", "notice": notice, "pool_entry": entry}));
    }
    let (runtime, model, request_json, status, failure_kind): (
        String,
        String,
        String,
        String,
        Option<String>,
    ) = tx.query_row(
        "SELECT runtime,model,request_json,status,failure_kind FROM agents WHERE id=?",
        [agent_id],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    let status: agent_run_domain::domain::Status = status.parse()?;
    let effort = bounded(
        serde_json::from_str::<Value>(&request_json)
            .ok()
            .and_then(|value| value.get("effort")?.as_str().map(str::to_owned)),
    );
    let failure_kind = matches!(status.as_str(), "failed" | "timed_out" | "lost")
        .then(|| bounded(failure_kind))
        .flatten();
    if !status.terminal() || delivery_id.trim().is_empty() || delivery_id.len() > 512 {
        return Err(invalid("invalid completion lifecycle fields"));
    }
    let root: AgentId = root.parse()?;
    let run: AgentId = agent_id.parse()?;
    Ok(json!({
        "kind": "completion",
        "notice": {
            "notification_id": delivery_id,
            "agent_id": root,
            "run_id": run,
            "status": status.as_str(),
            "runtime": bounded(Some(runtime)),
            "model": bounded(Some(model)),
            "effort": effort,
            "failure_kind": failure_kind,
        }
    }))
}

impl Store<'_> {
    /// Leases the oldest due delivery for `op.owner` (row 52), expiring
    /// unbound notices first. A known unsupported relay capability defers
    /// the row; a Claude notice whose previous attempt may have been sent
    /// ends failed-ambiguous without a new send; a malformed stored payload
    /// ends only its own row. Returns the lease and validated payload facts,
    /// or `None` when nothing was leased.
    pub fn claim_delivery_lease(
        &mut self,
        op: &crate::ops::ClaimDelivery,
    ) -> Result<Option<Value>> {
        let time = op.at;
        let policy = &op.policy;
        if !time.is_finite() || op.owner.is_empty() || op.owner.len() > 256 {
            return Err(invalid("invalid delivery claim"));
        }
        let lease = if policy.lease_seconds.is_finite() && policy.lease_seconds > 0.0 {
            policy.lease_seconds
        } else {
            DEFAULT_LEASE_SECONDS
        };
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        tx.execute(
            "UPDATE deliveries SET state='expired',lease_owner=NULL,lease_until=NULL,next_attempt_at=NULL \
             WHERE state='waiting_binding' AND id NOT IN (SELECT completion_delivery_id FROM pools WHERE completion_delivery_id IS NOT NULL) \
             AND agent_id IN (SELECT id FROM agents WHERE status IN ('succeeded','failed','timed_out','cancelled','lost')) \
             AND terminal_event_seq IN (SELECT seq FROM events WHERE at<=?)",
            [time - BINDING_WINDOW_SECONDS],
        )?;
        let found: Option<(String, String, String, u32)> = tx
            .query_row(
                "SELECT id,agent_id,orchestrator_session_id,attempts FROM deliveries \
                 WHERE (state IN ('pending','retry_wait') AND COALESCE(next_attempt_at,0)<=?1) OR (state='sending' AND lease_until<=?1) \
                 ORDER BY COALESCE(next_attempt_at,lease_until,0),id LIMIT 1",
                [time],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some((delivery_id, agent_id, session_id, attempts)) = found else {
            tx.commit()?;
            return Ok(None);
        };
        let (transport, session): (String, String) = tx.query_row(
            "SELECT transport,external_session_id FROM orchestrator_sessions WHERE id=?",
            [session_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let transport =
            agent_run_domain::domain::OrchestratorRef::canonical_transport_name(&transport)
                .map(str::to_owned)
                .unwrap_or(transport);
        let attention: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM events e JOIN deliveries d ON d.terminal_event_seq=e.seq WHERE d.id=? AND e.kind='pool_join_needs_action')",
            [&delivery_id],
            |row| row.get(0),
        )?;
        if transport == "codex_queue" && attention && !op.attention_peer {
            tx.execute(
                "UPDATE deliveries SET state='retry_wait',next_attempt_at=?,last_error='relay_v5_required' WHERE id=?",
                params![
                    time + policy.retry_base_seconds.max(1.0).min(policy.retry_cap_seconds),
                    delivery_id
                ],
            )?;
            tx.commit()?;
            return Ok(None);
        }
        if transport == "claude_uds" && attempts > 0 {
            let last: Option<String> = tx
                .query_row(
                    "SELECT json_extract(evidence_json,'$.classifier') FROM delivery_attempt_evidence WHERE delivery_id=? AND attempt=?",
                    params![delivery_id, attempts],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();
            let classifier = match last.as_deref() {
                Some(known @ ("uds_unconfirmed" | "uds_ambiguous")) => Some(known),
                None => Some("uds_unconfirmed"),
                Some(_) => None,
            };
            if let Some(classifier) = classifier {
                tx.execute(
                    "UPDATE deliveries SET state='failed',lease_owner=NULL,lease_until=NULL,next_attempt_at=NULL,last_error=?,ambiguous_result=1 WHERE id=?",
                    params![classifier, delivery_id],
                )?;
                tx.commit()?;
                return Ok(None);
            }
        }
        let payload = match claim_payload(&tx, &delivery_id, &agent_id) {
            Ok(payload) => payload,
            Err(agent_run_domain::Error::Validation(_) | agent_run_domain::Error::Integrity(_)) => {
                tx.execute(
                    "UPDATE deliveries SET state='failed',lease_owner=NULL,lease_until=NULL,next_attempt_at=NULL,last_error='invalid_payload' WHERE id=?",
                    [&delivery_id],
                )?;
                tx.commit()?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let attempt = attempts
            .checked_add(1)
            .ok_or_else(|| invalid("delivery attempt counter overflow"))?;
        let updated = tx.execute(
            "UPDATE deliveries SET state='sending',attempts=?,lease_owner=?,lease_until=?,next_attempt_at=NULL \
             WHERE id=? AND ((state IN ('pending','retry_wait') AND COALESCE(next_attempt_at,0)<=?5) OR (state='sending' AND lease_until<=?5))",
            params![attempt, op.owner, time + lease, delivery_id, time],
        )?;
        tx.commit()?;
        Ok((updated == 1).then(|| {
            json!({
                "delivery_id": delivery_id,
                "lease_owner": op.owner,
                "attempt": attempt,
                "transport": transport,
                "session": session,
                "payload": payload,
            })
        }))
    }

    /// Records one attempt outcome under its live lease (row 53): accepted
    /// delivers; an exhausted attempt budget, a possibly-sent Claude
    /// observation or an unsupported transport fails; anything else waits
    /// for a bounded exponential retry. Queue evidence is stored once per
    /// attempt. Returns the new state, or `not_owned` when the lease was lost
    /// (no change).
    pub fn complete_delivery_attempt(
        &mut self,
        done: &crate::ops::DeliveryCompletion,
    ) -> Result<String> {
        let policy = &done.policy;
        if !done.at.is_finite() || done.classifier.is_empty() || done.classifier.len() > 64 {
            return Err(invalid("invalid delivery completion"));
        }
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let owns: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM deliveries WHERE id=? AND state='sending' AND lease_owner=? AND attempts=? AND lease_until>?)",
            params![done.delivery_id, done.owner, done.attempt, done.at],
            |row| row.get(0),
        )?;
        if !owns {
            tx.commit()?;
            return Ok("not_owned".into());
        }
        let exhausted = policy.max_attempts > 0 && done.attempt >= policy.max_attempts;
        let possibly_sent = done.transport == "claude_uds"
            && matches!(
                done.classifier.as_str(),
                "uds_unconfirmed" | "uds_ambiguous"
            );
        let failed = !done.accepted
            && (exhausted || possibly_sent || done.classifier == "unsupported_transport");
        let state = if done.accepted {
            "delivered"
        } else if failed {
            "failed"
        } else {
            "retry_wait"
        };
        let next_attempt = (state == "retry_wait").then(|| {
            done.at
                + (policy.retry_base_seconds
                    * 2f64.powi(done.attempt.saturating_sub(1).min(20) as i32))
                .min(policy.retry_cap_seconds)
        });
        if matches!(done.transport.as_str(), "codex_queue" | "claude_uds")
            && let Some(evidence) = &done.evidence
        {
            let text = serde_json::to_string(evidence)?;
            if text.len() > MAX_EVIDENCE_BYTES {
                return Err(invalid("delivery evidence exceeds its byte bound"));
            }
            tx.execute(
                "INSERT OR IGNORE INTO delivery_attempt_evidence(delivery_id,attempt,recorded_at,evidence_json) VALUES(?,?,?,?)",
                params![done.delivery_id, done.attempt, done.at, text],
            )?;
        }
        tx.execute(
            "UPDATE deliveries SET state=?,lease_owner=NULL,lease_until=NULL,next_attempt_at=?,last_error=?,ambiguous_result=MAX(ambiguous_result,?) WHERE id=?",
            params![
                state,
                next_attempt,
                (!done.accepted).then_some(done.classifier.as_str()),
                done.ambiguous,
                done.delivery_id
            ],
        )?;
        tx.commit()?;
        Ok(state.to_owned())
    }
}
