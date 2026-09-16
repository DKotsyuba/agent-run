//! Durable completion-delivery rows and immutable queue-attempt evidence.
//!
//! These helpers keep terminal notices, late orchestrator binding, and evidence
//! verdicts within SQLite transactions owned by the calling [`Store`].

use crate::Store;
use agent_run_domain::{
    domain::{now, AgentId},
    error::invalid,
    Result,
};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{json, Map, Value};

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
    let tx = store
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
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
    tx: &Transaction<'_>,
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
    tx: &Transaction<'_>,
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
    let tx = store
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    owned_attempt(&tx, delivery_id, owner, at, evidence)?;
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
    let tx = store
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    owned_attempt(&tx, delivery_id, owner, at, evidence)?;
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
    let tx = store
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let attempts = owned_attempt(&tx, delivery_id, owner, at, evidence)?;
    let exponent = attempts.saturating_sub(1).min(20);
    let delay = (base_delay * 2_f64.powi(exponent as i32)).min(max_delay);
    let next = at + delay;
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
    Ok(next)
}

/// Returns the latest validated evidence document for one delivery.
pub(crate) fn latest(store: &Store, delivery_id: &str) -> Result<Option<Value>> {
    required_text("delivery_id", delivery_id)?;
    let raw: Option<String> = store
        .conn
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

impl Store {
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
        let transaction = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE deliveries SET state='cancelled',lease_owner=NULL,lease_until=NULL,next_attempt_at=NULL WHERE id=? AND state NOT IN ('delivered','cancelled')",
            [delivery_id],
        )? == 1;
        transaction.commit()?;
        Ok(changed)
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
