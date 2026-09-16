//! Durable, at-least-once completion delivery without task or credential content.
pub mod claude;
pub mod relay;

use crate::{
    config::Config,
    domain::{now, AgentId, Status},
    error::invalid,
    state::Store,
    Result,
};
use fs2::FileExt;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fs::File, path::Path};

const LEASE_SECONDS: f64 = 30.0;
const MAX_TAIL_BYTES: usize = 4096;
const MAX_EVIDENCE_BYTES: usize = 16 * 1024;
const DEFAULT_MAX_BATCH: usize = 1000;

/// A terminal lifecycle notification containing only trusted identifiers and selectors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notice {
    /// Stable idempotency key for this notice.
    pub notification_id: String,
    /// Durable agent identifier whose result the recipient should inspect.
    pub agent_id: AgentId,
    /// Terminal status being reported.
    pub status: Status,
    /// Runtime selector captured at admission, when still displayable.
    pub runtime: Option<String>,
    /// Model selector captured at admission, when still displayable.
    pub model: Option<String>,
    /// Explicit effort selector, or absent when it was not specified.
    pub effort: Option<String>,
    /// Bounded classifier only; raw runtime failure prose is excluded.
    pub failure_kind: Option<String>,
}

/// Immutable, secret-safe facts recorded for one owned Codex queue attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    /// Allowlisted result classifier, not a host response body.
    pub classifier: String,
    /// Queue executable label, never an argv value or filesystem path.
    pub executable: String,
    /// Fixed argv roles only; never supplied argument values.
    pub argv_shape: Vec<String>,
    /// Wall-clock duration of the attempt in milliseconds.
    pub duration_ms: u64,
    /// Child exit status, if a child was successfully spawned.
    pub returncode: Option<i32>,
    /// Spawn errno, mutually exclusive with `returncode`.
    pub spawn_errno: Option<i32>,
    /// Bounded error class, if applicable.
    pub error_class: Option<String>,
    /// Redacted UTF-8 stdout suffix, capped to 4096 bytes.
    pub stdout_tail: String,
    /// Redacted UTF-8 stderr suffix, capped to 4096 bytes.
    pub stderr_tail: String,
    /// Original stdout byte count before bounding.
    pub stdout_bytes: u64,
    /// Original stderr byte count before bounding.
    pub stderr_bytes: u64,
    /// Whether stdout was shortened for persistence.
    pub stdout_truncated: bool,
    /// Whether stderr was shortened for persistence.
    pub stderr_truncated: bool,
    /// Whether a remote message identifier was observed, without storing it.
    pub message_id_present: bool,
}

impl Evidence {
    /// Creates a no-output relay observation suitable for an in-process transport.
    pub(crate) fn new(classifier: &str, accepted: bool, ambiguous: bool) -> Self {
        Self {
            classifier: classifier.into(),
            executable: "desktop-relay".into(),
            argv_shape: vec!["relay".into()],
            duration_ms: 0,
            returncode: None,
            spawn_errno: None,
            error_class: if accepted {
                None
            } else {
                Some(
                    if ambiguous {
                        "ambiguous"
                    } else {
                        "unavailable"
                    }
                    .into(),
                )
            },
            stdout_tail: String::new(),
            stderr_tail: String::new(),
            stdout_bytes: 0,
            stderr_bytes: 0,
            stdout_truncated: false,
            stderr_truncated: false,
            message_id_present: accepted,
        }
    }

    /// Returns whether this observation is an accepted remote acknowledgement.
    fn accepted(&self) -> bool {
        matches!(
            self.classifier.as_str(),
            "relay_accepted" | "uds_written" | "delivered"
        )
    }

    /// Returns whether this observation can have reached the peer without acknowledgement.
    fn ambiguous(&self) -> bool {
        matches!(
            self.classifier.as_str(),
            "relay_ambiguous" | "uds_ambiguous"
        )
    }

    /// Produces the only representation allowed into SQLite, redacting and bounding tails first.
    fn persisted(&self) -> Result<Self> {
        let mut value = self.clone();
        value.stdout_tail = redact_tail(&value.stdout_tail);
        value.stderr_tail = redact_tail(&value.stderr_tail);
        value.stdout_truncated |= value.stdout_tail.len() < self.stdout_tail.len();
        value.stderr_truncated |= value.stderr_tail.len() < self.stderr_tail.len();
        validate_evidence(&value)?;
        if serde_json::to_vec(&value)?.len() > MAX_EVIDENCE_BYTES {
            return Err(invalid("delivery evidence exceeds 16384 bytes"));
        }
        Ok(value)
    }
}

/// Validates a decoded evidence object and returns its persistence-safe form.
pub fn safe_evidence(raw: &Value) -> Option<Evidence> {
    let evidence: Evidence = serde_json::from_value(raw.clone()).ok()?;
    evidence.persisted().ok()
}

/// Converts untrusted diagnostic text into a redacted, valid UTF-8 suffix.
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
    while safe.len() > MAX_TAIL_BYTES {
        let mut drop = safe.len() - MAX_TAIL_BYTES;
        while !safe.is_char_boundary(drop) {
            drop += 1;
        }
        safe = safe[drop..].to_owned();
    }
    safe
}

/// Rejects evidence that is malformed, unsafe, or too large to be durable diagnostics.
fn validate_evidence(value: &Evidence) -> Result<()> {
    if value.classifier.trim().is_empty()
        || value.executable.trim().is_empty()
        || value.argv_shape.is_empty()
        || value
            .argv_shape
            .iter()
            .any(|part| part.trim().is_empty() || part.len() > 64)
        || (value.returncode.is_some() && value.spawn_errno.is_some())
        || value
            .error_class
            .as_deref()
            .is_some_and(|item| item.trim().is_empty() || item.len() > 128)
        || value.stdout_tail.len() > MAX_TAIL_BYTES
        || value.stderr_tail.len() > MAX_TAIL_BYTES
    {
        return Err(invalid("invalid delivery attempt evidence"));
    }
    Ok(())
}

/// Escapes metadata that could otherwise inject lines into the fixed notice template.
fn escaped(value: Option<&str>, missing: &str) -> String {
    let mut output = String::new();
    for character in value.unwrap_or(missing).chars() {
        let code = character as u32;
        if code < 0x20 || (0x7f..=0x9f).contains(&code) || matches!(code, 0x2028 | 0x2029) {
            output.push_str(&format!("\\u{code:04x}"));
        } else {
            output.push(character);
        }
    }
    output
}

/// Loads the frozen package-owned notice guidance for a terminal status and classifier.
fn guidance(status: Status, kind: Option<&str>) -> Result<Option<(String, String, String)>> {
    if !matches!(status, Status::Failed | Status::TimedOut | Status::Lost) {
        return Ok(None);
    }
    /// One package-owned failure reason and recovery advice pair.
    #[derive(Deserialize)]
    struct Pair {
        reason: String,
        advice: String,
    }
    /// The subset of the frozen JSON contract used by the renderer.
    #[derive(Deserialize)]
    struct Contract {
        default_failure: Pair,
        status_guidance: std::collections::BTreeMap<String, Pair>,
        failure_guidance: std::collections::BTreeMap<String, Pair>,
    }
    let contract: Contract =
        serde_json::from_str(include_str!("../../../../assets/completion_notice.json"))?;
    let display = escaped(kind, "unknown");
    let pair = if status == Status::TimedOut {
        contract
            .status_guidance
            .get("timed_out")
            .unwrap_or(&contract.default_failure)
    } else {
        contract
            .failure_guidance
            .get(kind.unwrap_or("unknown"))
            .or_else(|| contract.status_guidance.get(status.as_str()))
            .unwrap_or(&contract.default_failure)
    };
    Ok(Some((display, pair.reason.clone(), pair.advice.clone())))
}

impl Notice {
    /// Validates terminal notice facts while excluding tasks, answers, and failure prose.
    pub fn validate(&self) -> Result<()> {
        if !self.status.terminal()
            || self.notification_id.trim().is_empty()
            || self.notification_id.len() > 512
            || (matches!(self.status, Status::Succeeded | Status::Cancelled)
                && self.failure_kind.is_some())
        {
            return Err(invalid("invalid completion lifecycle fields"));
        }
        for value in [&self.runtime, &self.model, &self.effort, &self.failure_kind]
            .into_iter()
            .flatten()
        {
            if value.trim().is_empty() || value.chars().count() > 128 {
                return Err(invalid("invalid completion metadata"));
            }
        }
        Ok(())
    }

    /// Renders the exact frozen Python notice template using only safe contract text.
    pub fn render(&self) -> Result<String> {
        self.validate()?;
        let failure = guidance(self.status, self.failure_kind.as_deref())?
            .map(|(kind, reason, advice)| {
                format!("\n- Failure: {kind} — {reason}\n- Advice: {advice}")
            })
            .unwrap_or_default();
        Ok(format!(
            "agent-run/completion\n\n- ID: {}\n- Status: {}{}\n- Runtime/model: {}/{}:{}\n- Notice: [notification {} v1]",
            self.agent_id,
            self.status.as_str(),
            failure,
            escaped(self.runtime.as_deref(), "unknown"),
            escaped(self.model.as_deref(), "unknown"),
            escaped(self.effort.as_deref(), "unspecified"),
            self.notification_id,
        ))
    }
}

/// A claimed delivery and the private lease token that permits exactly one terminal update.
struct Claim {
    delivery_id: String,
    lease_owner: String,
    attempt: u32,
    transport: String,
    session: String,
    notice: Notice,
}

/// Counts one bounded outbox drain and reports whether another process owns it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DispatchResult {
    /// Number of notices claimed by this drain.
    pub claimed: usize,
    /// Number of claims completed successfully.
    pub delivered: usize,
    /// Number of claims left scheduled for retry.
    pub retried: usize,
    /// Number of claims made terminally failed.
    pub failed: usize,
    /// Number of attempts whose acceptance was ambiguous.
    pub ambiguous: usize,
    /// Number of claims whose lease was lost before completion.
    pub claim_lost: usize,
    /// Whether another drain already holds the dispatcher lock.
    pub locked_out: bool,
}

/// Opens the home-owned nonblocking dispatcher lock.
fn dispatcher_lock(home: &Path) -> Result<Option<File>> {
    let locks = home.join("locks");
    std::fs::create_dir_all(&locks)?;
    let file = File::options()
        .create(true)
        .append(true)
        .open(locks.join("delivery-dispatcher.lock"))?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(Some(file)),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Expires terminal deliveries that were never bound during the documented binding window.
fn expire_unbound(tx: &rusqlite::Transaction<'_>, time: f64) -> Result<()> {
    tx.execute(
        "UPDATE deliveries SET state='expired',lease_owner=NULL,lease_until=NULL,next_attempt_at=NULL \
         WHERE state='waiting_binding' AND agent_id IN (SELECT id FROM agents WHERE status IN ('succeeded','failed','timed_out','cancelled','lost')) \
         AND terminal_event_seq IN (SELECT seq FROM events WHERE at<=?)",
        [time - 3600.0],
    )?;
    Ok(())
}

/// Atomically selects and leases one due bound delivery for this dispatcher identity.
fn claim(home: &Path, owner: &str) -> Result<Option<Claim>> {
    let mut store = Store::open(home)?;
    let tx = store
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let time = now();
    expire_unbound(&tx, time)?;
    let found = tx
        .query_row(
            "SELECT id,agent_id,orchestrator_session_id,attempts FROM deliveries \
         WHERE (state IN ('pending','retry_wait') AND COALESCE(next_attempt_at,0)<=?) \
            OR (state='sending' AND lease_until<=?) \
         ORDER BY COALESCE(next_attempt_at,lease_until,0),id LIMIT 1",
            params![time, time],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u32>(3)?,
                ))
            },
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
    let (runtime, model, request_json, status, kind): (
        String,
        String,
        String,
        String,
        Option<String>,
    ) = tx.query_row(
        "SELECT runtime,model,request_json,status,failure_kind FROM agents WHERE id=?",
        [&agent_id],
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
    let bounded =
        |value: String| (!value.trim().is_empty() && value.chars().count() <= 128).then_some(value);
    let effort = serde_json::from_str::<Value>(&request_json)
        .ok()
        .and_then(|value| value.get("effort")?.as_str().map(str::to_owned))
        .and_then(bounded);
    let notice = Notice {
        notification_id: delivery_id.clone(),
        agent_id: agent_id.parse()?,
        status: status.parse()?,
        runtime: bounded(runtime),
        model: bounded(model),
        effort,
        failure_kind: kind.and_then(bounded),
    };
    notice.validate()?;
    let attempt = attempts
        .checked_add(1)
        .ok_or_else(|| invalid("delivery attempt counter overflow"))?;
    let updated = tx.execute(
        "UPDATE deliveries SET state='sending',attempts=?,lease_owner=?,lease_until=?,next_attempt_at=NULL \
         WHERE id=? AND ((state IN ('pending','retry_wait') AND COALESCE(next_attempt_at,0)<=?) OR (state='sending' AND lease_until<=?))",
        params![attempt, owner, time + LEASE_SECONDS, delivery_id, time, time],
    )?;
    if updated != 1 {
        tx.commit()?;
        return Ok(None);
    }
    tx.commit()?;
    Ok(Some(Claim {
        delivery_id,
        lease_owner: owner.into(),
        attempt,
        transport,
        session,
        notice,
    }))
}

/// Persists one owned result and its immutable queue evidence in the same transaction.
fn complete(home: &Path, claim: &Claim, evidence: &Evidence) -> Result<()> {
    let config = Config::load(home)?;
    let mut store = Store::open(home)?;
    let tx = store
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let time = now();
    let owns: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM deliveries WHERE id=? AND state='sending' AND lease_owner=? AND attempts=? AND lease_until>?)",
        params![claim.delivery_id, claim.lease_owner, claim.attempt, time], |row| row.get(0),
    )?;
    if !owns {
        tx.commit()?;
        return Ok(());
    }
    let accepted = evidence.accepted();
    let ambiguous = evidence.ambiguous();
    let exhausted =
        config.delivery.max_attempts > 0 && claim.attempt >= config.delivery.max_attempts;
    let failed = !accepted && (exhausted || evidence.classifier == "unsupported_transport");
    let state = if accepted {
        "delivered"
    } else if failed {
        "failed"
    } else {
        "retry_wait"
    };
    let next_attempt = if state == "retry_wait" {
        Some(
            time + (config.delivery.retry_base_seconds
                * 2f64.powi((claim.attempt.saturating_sub(1)).min(20) as i32))
            .min(config.delivery.retry_cap_seconds),
        )
    } else {
        None
    };
    if claim.transport == "codex_queue" {
        let persisted = evidence.persisted()?;
        tx.execute(
            "INSERT OR IGNORE INTO delivery_attempt_evidence(delivery_id,attempt,recorded_at,evidence_json) VALUES(?,?,?,?)",
            params![claim.delivery_id, claim.attempt, time, serde_json::to_string(&persisted)?],
        )?;
    }
    tx.execute(
        "UPDATE deliveries SET state=?,lease_owner=NULL,lease_until=NULL,next_attempt_at=?,last_error=?,ambiguous_result=MAX(ambiguous_result,?) WHERE id=?",
        params![state, next_attempt, if accepted { None } else { Some(evidence.classifier.as_str()) }, ambiguous, claim.delivery_id],
    )?;
    tx.commit()?;
    Ok(())
}

/// Claims and attempts one completion delivery, using an ephemeral lease owner identity.
async fn dispatch_one(home: &Path) -> Result<Option<(String, bool)>> {
    let owner = format!(
        "disp-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    );
    let Some(claim) = claim(home, &owner)? else {
        return Ok(None);
    };
    let started = std::time::Instant::now();
    let mut evidence = match claim.transport.as_str() {
        "codex_queue" => relay::send(home, &claim.session, &claim.notice).await,
        "claude_uds" => claude::send(&claude_registry(), &claim.session, &claim.notice).await,
        _ => Evidence::new("unsupported_transport", false, false),
    };
    evidence.duration_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    complete(home, &claim, &evidence)?;
    let result = Store::open(home)?.conn.query_row(
        "SELECT state,ambiguous_result FROM deliveries WHERE id=?",
        [&claim.delivery_id],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?)),
    )?;
    Ok(Some(result))
}

/// Claims and attempts one completion delivery without draining the backlog.
pub async fn dispatch_once(home: &Path) -> Result<usize> {
    let Some(_lock) = dispatcher_lock(home)? else {
        return Ok(0);
    };
    Ok(dispatch_one(home).await?.is_some() as usize)
}

/// Drains at most `max_batch` due notices while holding one nonblocking lock.
pub async fn dispatch_with_batch(home: &Path, max_batch: usize) -> Result<DispatchResult> {
    if max_batch == 0 {
        return Err(invalid("max_batch must be at least 1"));
    }
    let Some(_lock) = dispatcher_lock(home)? else {
        return Ok(DispatchResult {
            locked_out: true,
            ..DispatchResult::default()
        });
    };
    let mut result = DispatchResult::default();
    while result.claimed < max_batch {
        let Some((state, ambiguous)) = dispatch_one(home).await? else {
            break;
        };
        result.claimed += 1;
        result.ambiguous += ambiguous as usize;
        match state.as_str() {
            "delivered" => result.delivered += 1,
            "retry_wait" => result.retried += 1,
            "failed" => result.failed += 1,
            _ => result.claim_lost += 1,
        }
    }
    Ok(result)
}

/// Drains the due delivery outbox using the default bounded batch size.
pub async fn dispatch(home: &Path) -> Result<DispatchResult> {
    dispatch_with_batch(home, DEFAULT_MAX_BATCH).await
}

/// Resolves the Claude session registry, permitting a process-scoped test override.
fn claude_registry() -> std::path::PathBuf {
    std::env::var_os("AGENT_RUN_CLAUDE_SESSION_REGISTRY")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(|home| std::path::PathBuf::from(home).join(".claude/sessions"))
        })
        .unwrap_or_default()
}
