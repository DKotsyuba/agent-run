//! Durable completion-delivery rows and immutable queue-attempt evidence.
//!
//! These helpers keep terminal notices, late orchestrator binding, and evidence
//! verdicts within SQLite transactions owned by the calling [`Store`].

use crate::Store;
use agent_run_domain::{
    domain::{now, AgentId, OrchestratorRef},
    error::invalid,
    Error, Result,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{Map, Value};

/// Maximum age of an unbound completion notice before it is permanently expired.
pub const BINDING_WINDOW_SECONDS: f64 = 3600.0;
/// Maximum UTF-8 bytes retained in one persisted stdout or stderr evidence tail.
pub const MAX_EVIDENCE_TAIL_BYTES: usize = 4096;
/// Maximum UTF-8 bytes retained in one complete evidence JSON document.
pub const MAX_EVIDENCE_BYTES: usize = 16 * 1024;

/// Inserts the one durable completion notice associated with a terminal event.
///
/// A terminal run with an already-known orchestrator session is immediately
/// `pending`; an otherwise equivalent run is `waiting_binding` until a later
/// [`Store::bind_orchestrator`] attaches that session. The caller owns the
/// surrounding transaction, so the notice cannot become visible without the
/// terminal event it references.
pub(crate) fn insert_terminal_notice(
    tx: &Transaction<'_>,
    id: &AgentId,
    session: Option<&str>,
    terminal_event_seq: i64,
    at: f64,
) -> Result<String> {
    let notification_id = format!("ntf_{}", uuid::Uuid::new_v4().simple());
    let (state, next_attempt_at) = if session.is_some() {
        ("pending", Some(at))
    } else {
        ("waiting_binding", None)
    };
    tx.execute(
        "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) VALUES(?,?,?,?,?,?)",
        params![notification_id, id.as_str(), session, terminal_event_seq, state, next_attempt_at],
    )?;
    Ok(notification_id)
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

impl Store {
    /// Attaches an orchestrator session and makes any waiting completion notice dispatchable.
    pub fn bind_orchestrator(
        &mut self,
        id: &AgentId,
        reference: &OrchestratorRef,
    ) -> Result<String> {
        reference.validate()?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM agents WHERE id=?)",
            [id.as_str()],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(Error::NotFound(id.to_string()));
        }
        let session_id = format!("os-{}", uuid::Uuid::new_v4().simple());
        let at = now();
        tx.execute(
            "INSERT INTO orchestrator_sessions(id,transport,external_session_id,external_turn_id,created_at,last_seen_at) VALUES(?,?,?,?,?,?) ON CONFLICT(transport,external_session_id) DO UPDATE SET external_turn_id=excluded.external_turn_id,last_seen_at=excluded.last_seen_at",
            params![session_id, reference.transport, reference.external_session_id, reference.external_turn_id, at, at],
        )?;
        let session_id: String = tx.query_row(
            "SELECT id FROM orchestrator_sessions WHERE transport=? AND external_session_id=?",
            params![reference.transport, reference.external_session_id],
            |row| row.get(0),
        )?;
        let changed = tx.execute(
            "UPDATE agents SET orchestrator_session_id=? WHERE id=? AND orchestrator_session_id IS NULL",
            params![session_id, id.as_str()],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.execute(
            "UPDATE deliveries SET orchestrator_session_id=?,state='pending',next_attempt_at=? WHERE agent_id=? AND state='waiting_binding'",
            params![session_id, at, id.as_str()],
        )?;
        tx.commit()?;
        Ok(session_id)
    }

    /// Expires waiting completion notices older than the documented binding window.
    pub fn expire_unbound_deliveries(&mut self, at: f64) -> Result<Vec<String>> {
        if !at.is_finite() {
            return Err(invalid("expiry time must be finite"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut statement = tx.prepare(
            "SELECT d.id FROM deliveries d JOIN agents a ON a.id=d.agent_id JOIN events e ON e.seq=d.terminal_event_seq WHERE d.state='waiting_binding' AND a.status IN ('succeeded','failed','timed_out','cancelled','lost') AND e.at<=? ORDER BY e.at,d.id",
        )?;
        let ids = statement
            .query_map([at - BINDING_WINDOW_SECONDS], |row| row.get::<_, String>(0))?
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
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
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
