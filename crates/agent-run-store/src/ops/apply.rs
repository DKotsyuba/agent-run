//! One-transaction execution of a typed operation with its durable receipt.
//!
//! [`execute`] runs on the writer thread with the sole read-write connection:
//! `BEGIN IMMEDIATE`, receipt lookup (identical replay answers the stored
//! outcome without mutating), operation identity and durable producer order,
//! the static authority matrix and in-transaction scope predicates, the
//! operation inside a savepoint, the receipt insert and `COMMIT`. Replies are
//! produced only after `COMMIT`; an uncertain commit or rollback is reported
//! as unknown, never as success or rollback.

use super::{EnvelopeHeader, OpKindRef, dispatch::Facts};
use crate::Store;
use agent_run_domain::{
    Error, Result,
    canonical::dumps,
    domain::{AgentId, now},
    error::PersistenceOperation,
    storage::{
        MAX_RESULT_BYTES, OperationId, ProducerId, RejectCode, RejectStage, StorageRejection,
        SubmitResult, UnknownCause, WriteAuthority, WriteReceipt,
    },
};
use rusqlite::{Connection, ErrorCode, OptionalExtension, params};
use serde_json::Value;
use std::time::Instant;

#[cfg(test)]
thread_local! {
    /// One-shot worker-thread pause inside the actual watermark SELECT progress hook.
    /// Tests install it on their owned connection thread; production has no seam.
    pub(crate) static WATERMARK_READ_PAUSE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    /// Whether this thread is currently reading watermark candidates.
    static READING_WATERMARKS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Facts of one submission the transaction needs besides the payload.
pub(crate) struct Submission<'a> {
    /// Envelope header.
    pub header: &'a EnvelopeHeader,
    /// Inventory row.
    pub kind: OpKindRef,
    /// Execution named by the payload, when it names one.
    pub execution: Option<AgentId>,
    /// Attempt named by the payload, when it names one.
    pub attempt: Option<String>,
    /// Service generation named by the payload, when it names one.
    pub generation: Option<String>,
    /// Payload facts checked against durable rows inside the transaction.
    pub checks: Checks,
    /// Current boot identifier.
    pub boot_id: &'a str,
    /// Current monotonic nanoseconds.
    pub now_ns: u64,
    /// Writer epoch.
    pub epoch: u64,
}

/// Payload facts checked inside the transaction besides the target.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Checks {
    /// Process owner `(kind, id)` whose execution/attempt/generation scope is
    /// resolved from its row (row 17).
    pub owner: Option<(String, String)>,
    /// Process identity the payload records (rows 12, 74 and 75); it must be the
    /// authenticated principal's own identity.
    pub claimed: Option<Claimed>,
    /// Account the payload names (row 31); it must be the account selected
    /// by the supervisor's pinned owned attempt.
    pub account: Option<String>,
    /// Runtime home whose recorded layout owner is the authorization target
    /// (row 89), read inside the transaction.
    pub layout: Option<String>,
}

/// A process identity proposed by a payload for recording.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Claimed {
    /// Proposed pid.
    pub pid: i32,
    /// Proposed identity token.
    pub token: String,
    /// Proposed birth time.
    pub birth: Option<f64>,
}

/// Why the writer must enter the degraded state after an outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Degradation {
    /// `BEGIN IMMEDIATE` was busy: an unknown writer holds the lock.
    UnknownWriter,
    /// `COMMIT` failed in a way that may have committed.
    CommitUncertain,
    /// `ROLLBACK` failed or autocommit could not be confirmed.
    RollbackUncertain,
}

/// Outcome of the internal `execute` operation plus an optional degradation signal.
pub struct Executed {
    /// Outcome for the caller.
    pub result: SubmitResult,
    /// Degradation to enter, if any.
    pub degraded: Option<Degradation>,
}

impl Executed {
    /// An outcome without degradation.
    fn plain(result: SubmitResult) -> Self {
        Self {
            result,
            degraded: None,
        }
    }
}

/// A refusal at `stage` with a closed message, not stored.
fn refuse(code: RejectCode, stage: RejectStage, message: &str) -> SubmitResult {
    SubmitResult::Rejected(StorageRejection::new(code, stage, message))
}

/// Stored receipt row.
struct StoredRow {
    /// Sequence.
    commit_seq: i64,
    /// Digest.
    digest: String,
    /// Kind name.
    op_kind: String,
    /// Producer key (canonical JSON).
    producer_key: String,
    /// Ordinal.
    ordinal: i64,
    /// Authority fingerprint.
    fingerprint: String,
    /// Scope.
    agent_id: Option<String>,
    /// `committed` or `rejected`.
    outcome: String,
    /// Result JSON.
    result_json: String,
    /// Epoch.
    writer_epoch: i64,
    /// Commit time.
    committed_at: f64,
}

/// Reads the stored receipt of `id`, if any.
fn stored(conn: &Connection, id: &OperationId) -> Result<Option<StoredRow>> {
    Ok(conn
        .query_row(
            "SELECT commit_seq,digest,op_kind,producer_key,ordinal,authority_fingerprint,agent_id,outcome,result_json,writer_epoch,committed_at \
             FROM operation_receipts WHERE operation_id=?",
            [id.as_str()],
            |row| {
                Ok(StoredRow {
                    commit_seq: row.get(0)?,
                    digest: row.get(1)?,
                    op_kind: row.get(2)?,
                    producer_key: row.get(3)?,
                    ordinal: row.get(4)?,
                    fingerprint: row.get(5)?,
                    agent_id: row.get(6)?,
                    outcome: row.get(7)?,
                    result_json: row.get(8)?,
                    writer_epoch: row.get(9)?,
                    committed_at: row.get(10)?,
                })
            },
        )
        .optional()?)
}

/// Rebuilds the caller-visible outcome of a stored receipt.
fn replay(id: &OperationId, row: StoredRow, replayed: bool) -> Result<SubmitResult> {
    if row.outcome == "rejected" {
        let mut refusal: StorageRejection = serde_json::from_str(&row.result_json)?;
        refusal.durable = true;
        return Ok(SubmitResult::Rejected(refusal));
    }
    let producer: ProducerId = serde_json::from_str(&row.producer_key)?;
    Ok(SubmitResult::Committed(WriteReceipt {
        operation_id: id.clone(),
        digest: agent_run_domain::storage::PayloadDigest::parse(&row.digest)?,
        op_kind: row.op_kind,
        producer,
        ordinal: row.ordinal as u64,
        scope: row.agent_id.map(|id| id.parse()).transpose()?,
        writer_epoch: row.writer_epoch as u64,
        commit_seq: row.commit_seq as u64,
        committed_at: row.committed_at,
        replayed,
        result: serde_json::from_str(&row.result_json)?,
    }))
}

/// Looks up the stored outcome of `id` for `authority` (status/replay path).
/// Only an identical fingerprint sees the receipt; anything else is `None`.
pub fn lookup(
    conn: &Connection,
    id: &OperationId,
    authority: &WriteAuthority,
) -> Result<Option<SubmitResult>> {
    match stored(conn, id)? {
        Some(row) if row.fingerprint == authority.fingerprint() => replay(id, row, true).map(Some),
        _ => Ok(None),
    }
}

/// Resolve only the original authority and payload, including durable refusals.
/// A different digest cannot observe or replay the original outcome.
pub(crate) fn lookup_digest(
    conn: &Connection,
    id: &OperationId,
    authority: &WriteAuthority,
    digest: &agent_run_domain::storage::PayloadDigest,
) -> Result<Option<SubmitResult>> {
    match stored(conn, id)? {
        Some(row)
            if row.fingerprint == authority.fingerprint() && row.digest == digest.as_str() =>
        {
            replay(id, row, true).map(Some)
        }
        _ => Ok(None),
    }
}

/// Whether a recorded birth time is exactly the authenticated birth time
/// (no tolerance: any difference is another process instant).
fn same_birth(left: Option<f64>, right: f64) -> bool {
    left == Some(right)
}

/// Checks a supervisor authority against the execution row: capability hash,
/// lineage root and either the recorded supervisor identity (`own`) or, when
/// `allow_start` and no supervisor is recorded yet, the startup owner.
fn supervisor_scope(
    conn: &Connection,
    authority: &WriteAuthority,
    execution: &AgentId,
    allow_start: bool,
) -> Result<std::result::Result<(), RejectCode>> {
    let WriteAuthority::Supervisor {
        logical,
        execution: own,
        capability_sha256,
        pid,
        birth,
        token,
        ..
    } = authority
    else {
        return Ok(Ok(()));
    };
    if own != execution {
        return Ok(Err(RejectCode::Unauthorized));
    }
    type Row = (
        Option<String>,
        String,
        Option<i32>,
        Option<String>,
        Option<f64>,
        Option<String>,
        Option<f64>,
    );
    let row: Option<Row> = conn
        .query_row(
            "SELECT supervisor_capability_sha256,CASE WHEN root_agent_id='' THEN id ELSE root_agent_id END,\
             supervisor_pid,supervisor_identity,supervisor_birth_time,startup_owner_pid_identity,startup_owner_birth_time \
             FROM agents WHERE id=?",
            [execution.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
        )
        .optional()?;
    let Some((capability, root, sup_pid, sup_token, sup_birth, startup, startup_birth)) = row
    else {
        return Ok(Err(RejectCode::UnknownScope));
    };
    if capability.as_deref() != Some(capability_sha256.as_str()) || root != logical.as_str() {
        return Ok(Err(RejectCode::StaleAuthority));
    }
    if sup_pid == Some(*pid)
        && sup_token.as_deref() == Some(token.as_str())
        && same_birth(sup_birth, *birth)
    {
        return Ok(Ok(()));
    }
    if allow_start && sup_pid.is_none() {
        let owner = startup.as_deref().unwrap_or_default();
        let matches = match serde_json::from_str::<Value>(owner) {
            Ok(evidence) if evidence.is_object() => {
                evidence["pid"].as_i64() == Some(i64::from(*pid))
                    && evidence["token"].as_str() == Some(token.as_str())
                    && same_birth(evidence["birth"].as_f64().or(startup_birth), *birth)
            }
            _ => owner == token && same_birth(startup_birth, *birth),
        };
        if matches {
            return Ok(Ok(()));
        }
    }
    Ok(Err(RejectCode::StaleAuthority))
}

/// Execution, attempt and generation scope of a process owner, read inside
/// the transaction; `None` when the owner row does not exist. An unknown
/// owner kind has no scope (only the broker may then reach the helper, which
/// refuses it).
fn owner_scope(conn: &Connection, kind: &str, owner: &str) -> Result<Option<Facts>> {
    Ok(match kind {
        "attempt" => conn
            .query_row("SELECT agent_id FROM attempts WHERE id=?", [owner], |row| {
                row.get::<_, String>(0)
            })
            .optional()?
            .map(|agent| (agent.parse::<AgentId>().ok(), Some(owner.to_owned()), None)),
        "service" => conn
            .query_row(
                "SELECT id FROM managed_service_generations WHERE id=?",
                [owner],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|generation| (None, None, Some(generation))),
        "probe" => conn
            .query_row(
                "SELECT generation_id FROM managed_service_probes WHERE id=?",
                [owner],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|generation| (None, None, Some(generation))),
        _ => Some((None, None, None)),
    })
}

/// Rows whose store unit acts on the execution's current owned attempt
/// without naming it; that attempt is derived inside the transaction and
/// must be the supervisor's pinned attempt. With no owned attempt these
/// units change no attempt state.
fn derives_owned_attempt(kind: OpKindRef) -> bool {
    matches!(
        kind,
        OpKindRef::WorkerTurn
            | OpKindRef::EnqueueNativeWake
            | OpKindRef::ObserveWorkerFinishReceipt
            | OpKindRef::RuntimeSession
    )
}

/// Checks that an attempt-targeted supervisor payload names exactly the
/// authority's pinned attempt (the legacy `<execution>:1` attempt when none
/// is pinned) and that this attempt belongs to the execution.
fn pinned_attempt(
    conn: &Connection,
    authority: &WriteAuthority,
    execution: &AgentId,
    attempt: Option<&str>,
) -> Result<std::result::Result<(), RejectCode>> {
    let (
        WriteAuthority::Supervisor {
            attempt: pinned, ..
        },
        Some(attempt),
    ) = (authority, attempt)
    else {
        return Ok(Ok(()));
    };
    let expected = pinned
        .clone()
        .unwrap_or_else(|| format!("{}:1", execution.as_str()));
    if attempt != expected {
        return Ok(Err(RejectCode::Unauthorized));
    }
    let owner: Option<String> = conn
        .query_row(
            "SELECT agent_id FROM attempts WHERE id=?",
            [attempt],
            |row| row.get(0),
        )
        .optional()?;
    Ok(match owner {
        None => Err(RejectCode::UnknownScope),
        Some(owner) if owner == execution.as_str() => Ok(()),
        Some(_) => Err(RejectCode::Unauthorized),
    })
}

/// Checks that `account` is exactly the account selected by the supervisor's
/// pinned owned attempt of `execution` (the legacy `<execution>:1` attempt
/// when none is pinned); any other or absent selection refuses.
fn pinned_account(
    conn: &Connection,
    authority: &WriteAuthority,
    execution: &AgentId,
    account: &str,
) -> Result<std::result::Result<(), RejectCode>> {
    let WriteAuthority::Supervisor {
        attempt: pinned, ..
    } = authority
    else {
        return Ok(Err(RejectCode::Unauthorized));
    };
    let attempt = pinned
        .clone()
        .unwrap_or_else(|| format!("{}:1", execution.as_str()));
    let selected: Option<Option<String>> = conn
        .query_row(
            "SELECT selected_account_id FROM attempts WHERE id=? AND agent_id=? AND ownership_active=1",
            [attempt.as_str(), execution.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    Ok(match selected.flatten() {
        Some(selected) if selected == account => Ok(()),
        _ => Err(RejectCode::Unauthorized),
    })
}

/// Checks that a proposed process identity is exactly the authenticated
/// supervisor or service-executor process (pid, token and birth).
fn claimed_is_principal(authority: &WriteAuthority, claimed: &Claimed) -> bool {
    let (pid, token, birth) = match authority {
        WriteAuthority::Supervisor {
            pid, token, birth, ..
        }
        | WriteAuthority::ServiceExec {
            pid, token, birth, ..
        } => (pid, token, birth),
        _ => return false,
    };
    claimed.pid == *pid && claimed.token == *token && same_birth(claimed.birth, *birth)
}

/// Checks a service-executor authority against its generation row.
fn service_scope(
    conn: &Connection,
    authority: &WriteAuthority,
    generation: Option<&str>,
) -> Result<std::result::Result<(), RejectCode>> {
    let WriteAuthority::ServiceExec {
        generation: own,
        capability_sha256,
        pid,
        birth,
        token,
        root_pid,
        root_birth,
        root_token,
    } = authority
    else {
        return Ok(Ok(()));
    };
    if generation != Some(own.as_str()) {
        return Ok(Err(RejectCode::Unauthorized));
    }
    let row: Option<(Option<String>, String, Option<String>)> = conn
        .query_row(
            "SELECT exec_capability_sha256,broker_identity_json,process_identity_json FROM managed_service_generations WHERE id=?",
            [own.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((capability, root, process)) = row else {
        return Ok(Err(RejectCode::UnknownScope));
    };
    let root: Value = serde_json::from_str(&root).unwrap_or(Value::Null);
    if capability.as_deref() != Some(capability_sha256.as_str())
        || root["pid"].as_i64() != Some(i64::from(*root_pid))
        || root["token"].as_str() != Some(root_token.as_str())
        || !same_birth(root["birth"].as_f64(), *root_birth)
    {
        return Ok(Err(RejectCode::StaleAuthority));
    }
    match process.map(|raw| serde_json::from_str::<Value>(&raw).unwrap_or(Value::Null)) {
        None => Ok(Ok(())),
        Some(identity)
            if identity["pid"].as_i64() == Some(i64::from(*pid))
                && identity["token"].as_str() == Some(token.as_str())
                && same_birth(identity["birth"].as_f64(), *birth) =>
        {
            Ok(Ok(()))
        }
        Some(_) => Ok(Err(RejectCode::StaleAuthority)),
    }
}

/// Checks a worker authority against the payload's execution and attempt.
fn worker_scope(
    authority: &WriteAuthority,
    execution: Option<&AgentId>,
    attempt: Option<&str>,
) -> std::result::Result<(), RejectCode> {
    match authority {
        WriteAuthority::Worker {
            execution: own,
            attempt: own_attempt,
        } if execution == Some(own) && attempt.is_none_or(|attempt| attempt == own_attempt) => {
            Ok(())
        }
        WriteAuthority::Worker { .. } => Err(RejectCode::Unauthorized),
        _ => Ok(()),
    }
}

/// Whether `error` is a storage-level failure (rolled back, not durable).
fn storage_failure(error: &Error) -> bool {
    match error {
        Error::Sql(_) | Error::Io(_) => true,
        Error::Persistence { source, .. } | Error::OwnershipCheckpoint { source, .. } => {
            storage_failure(source)
        }
        _ => false,
    }
}

/// Rolls back the top-level transaction and confirms autocommit.
fn rollback_confirmed(conn: &Connection) -> bool {
    conn.execute_batch("ROLLBACK").is_ok() && conn.is_autocommit()
}

/// Inserts one receipt row (and advances the producer watermark) inside the
/// open top-level transaction.
fn insert_receipt(
    conn: &Connection,
    sub: &Submission<'_>,
    authority: &WriteAuthority,
    scope: Option<&AgentId>,
    outcome: &str,
    result_json: &str,
    at: f64,
) -> Result<i64> {
    let header = sub.header;
    conn.execute(
        "INSERT INTO operation_receipts(operation_id,digest,op_kind,producer_key,ordinal,authority_kind,authority_fingerprint,agent_id,outcome,result_json,writer_epoch,committed_at) \
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        params![
            header.operation_id.as_str(),
            header.digest.as_str(),
            sub.kind.name(),
            header.producer.key(),
            header.ordinal as i64,
            authority.kind(),
            authority.fingerprint(),
            scope.map(AgentId::as_str),
            outcome,
            result_json,
            sub.epoch as i64,
            at
        ],
    )?;
    let seq = conn.last_insert_rowid();
    let execution = match header.producer.kind {
        agent_run_domain::storage::ProducerKind::Supervisor => {
            sub.execution.as_ref().map(AgentId::as_str)
        }
        _ => None,
    };
    conn.execute(
        "INSERT INTO writer_producers(producer_key,last_ordinal,last_boot_tag,last_issued_ns,execution_id,updated_at) VALUES(?,?,?,?,?,?) \
         ON CONFLICT(producer_key) DO UPDATE SET last_ordinal=excluded.last_ordinal,last_boot_tag=excluded.last_boot_tag, \
         last_issued_ns=MAX(last_issued_ns,excluded.last_issued_ns),execution_id=COALESCE(excluded.execution_id,execution_id),updated_at=excluded.updated_at",
        params![
            header.producer.key(),
            header.ordinal as i64,
            header.operation_id.boot_tag(),
            header.operation_id.issued_ns() as i64,
            execution,
            at
        ],
    )?;
    Ok(seq)
}

/// Decision taken inside the open transaction before the operation runs.
enum Prepared {
    /// Identical replay: answer the stored outcome, mutate nothing.
    Replay(Box<SubmitResult>),
    /// Non-durable refusal (nothing stored).
    Refuse(RejectCode, RejectStage, &'static str),
    /// Durable refusal stored as a rejected receipt.
    Durable(RejectCode, &'static str),
    /// Run the operation.
    Run,
}

/// Steps 2-4: receipt lookup, identity/order and scope predicates.
fn prepare(
    conn: &Connection,
    authority: &WriteAuthority,
    sub: &Submission<'_>,
) -> Result<Prepared> {
    let header = sub.header;
    if let Some(row) = stored(conn, &header.operation_id)? {
        let same = row.digest == header.digest.as_str()
            && row.producer_key == header.producer.key()
            && row.ordinal == header.ordinal as i64
            && row.fingerprint == authority.fingerprint();
        if !same {
            return Ok(Prepared::Refuse(
                RejectCode::ReplayConflict,
                RejectStage::Gate,
                "operation id was used for different content",
            ));
        }
        return Ok(Prepared::Replay(Box::new(replay(
            &header.operation_id,
            row,
            true,
        )?)));
    }
    if header.operation_id.is_stale(sub.boot_id, sub.now_ns) {
        return Ok(Prepared::Refuse(
            RejectCode::StaleOperation,
            RejectStage::Gate,
            "operation identity is stale",
        ));
    }
    let last: Option<i64> = conn
        .query_row(
            "SELECT last_ordinal FROM writer_producers WHERE producer_key=?",
            [header.producer.key()],
            |row| row.get(0),
        )
        .optional()?;
    if last.is_some_and(|last| header.ordinal as i64 <= last) {
        return Ok(Prepared::Refuse(
            RejectCode::OrderViolation,
            RejectStage::Gate,
            "producer ordinal is not increasing",
        ));
    }
    let (mut execution, mut attempt, mut generation) = (
        sub.execution.clone(),
        sub.attempt.clone(),
        sub.generation.clone(),
    );
    if let Some((kind, owner)) = &sub.checks.owner {
        let Some(scope) = owner_scope(conn, kind, owner)? else {
            return Ok(Prepared::Refuse(
                RejectCode::UnknownScope,
                RejectStage::Authorization,
                "process owner no longer exists",
            ));
        };
        (execution, attempt, generation) = scope;
        let allowed = match authority {
            WriteAuthority::Supervisor { .. } => kind == "attempt",
            WriteAuthority::ServiceExec { .. } => kind == "service",
            _ => true,
        };
        if !allowed {
            return Ok(Prepared::Durable(
                RejectCode::Unauthorized,
                "authority may not record this process owner kind",
            ));
        }
    }
    // A layout commit is authorized against the owner recorded with the
    // prepared layout: a supervisor only for its own, an unowned layout only
    // by the broker. An unregistered home changes nothing.
    if let Some(home) = &sub.checks.layout {
        let owner: Option<Option<String>> = conn
            .query_row(
                "SELECT owner_agent_id FROM runtime_storage_layouts WHERE runtime_home=?",
                [home],
                |row| row.get(0),
            )
            .optional()?;
        let Some(owner) = owner else {
            return Ok(Prepared::Refuse(
                RejectCode::UnknownScope,
                RejectStage::Authorization,
                "runtime storage layout is not registered",
            ));
        };
        execution = owner.and_then(|owner| owner.parse().ok());
    }
    // A supervisor's quota observation is scoped to its own execution.
    if execution.is_none()
        && sub.kind == OpKindRef::RecordQuotaSnapshot
        && let WriteAuthority::Supervisor { execution: own, .. } = authority
    {
        execution = Some(own.clone());
    }
    if attempt.is_none()
        && derives_owned_attempt(sub.kind)
        && let Some(execution) = &execution
    {
        attempt = conn
            .query_row(
                "SELECT id FROM attempts WHERE agent_id=? AND ownership_active=1 AND finished_at IS NULL",
                [execution.as_str()],
                |row| row.get(0),
            )
            .optional()?;
    }
    if sub.kind.scoped() {
        let exists = match &sub.execution {
            Some(execution) => conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM agents WHERE id=?)",
                [execution.as_str()],
                |row| row.get::<_, bool>(0),
            )?,
            None => false,
        };
        if !exists {
            // Non-durable: no receipt may reference a deleted execution.
            return Ok(Prepared::Refuse(
                RejectCode::UnknownScope,
                RejectStage::Authorization,
                "execution scope no longer exists",
            ));
        }
    }
    let checked = match authority {
        WriteAuthority::Supervisor { .. } => match &execution {
            Some(execution) => supervisor_scope(
                conn,
                authority,
                execution,
                matches!(
                    sub.kind,
                    OpKindRef::ClaimStartup | OpKindRef::RecordSupervisorOwner
                ),
            )?
            .and(pinned_attempt(
                conn,
                authority,
                execution,
                attempt.as_deref(),
            )?),
            None => Err(RejectCode::Unauthorized),
        },
        WriteAuthority::ServiceExec { .. } => {
            service_scope(conn, authority, generation.as_deref())?
        }
        WriteAuthority::Worker { .. } => {
            worker_scope(authority, execution.as_ref(), attempt.as_deref())
        }
        _ => Ok(()),
    };
    let checked = checked.and_then(|()| match &sub.checks.claimed {
        Some(claimed) if !claimed_is_principal(authority, claimed) => Err(RejectCode::Unauthorized),
        _ => Ok(()),
    });
    let checked = match (checked, &sub.checks.account, &execution) {
        (Ok(()), Some(account), Some(execution)) => {
            pinned_account(conn, authority, execution, account)?
        }
        (Ok(()), Some(_), None) => Err(RejectCode::Unauthorized),
        (checked, _, _) => checked,
    };
    Ok(match checked {
        Ok(()) => Prepared::Run,
        Err(RejectCode::UnknownScope) => Prepared::Refuse(
            RejectCode::UnknownScope,
            RejectStage::Authorization,
            "authority scope no longer exists",
        ),
        Err(code) => Prepared::Durable(code, "authority does not own this scope"),
    })
}

/// Closed persistence boundary of `kind` at `phase`: the truthful legacy
/// label where one exists, otherwise [`PersistenceOperation::WriterApply`].
fn boundary(kind: OpKindRef, phase: RejectStage) -> PersistenceOperation {
    use OpKindRef as K;
    use PersistenceOperation as P;
    match (kind, phase) {
        (K::RememberProcesses, RejectStage::Acquire) => P::OwnershipAcquire,
        (K::RememberProcesses, RejectStage::Commit) => P::OwnershipCommit,
        (K::RememberProcesses, _) => P::OwnershipWrite,
        (K::RuntimeSession, _) => P::RuntimeSession,
        (K::AppendJournal, _) => P::TranscriptAppend,
        (K::FinishTerminal, _) => P::TerminalCommit,
        (K::RetainCleanupEvidence | K::ProviderCleanup, _) => P::CleanupWrite,
        (K::ClaimCommand, _) => P::CommandClaim,
        (K::EnqueueCommand | K::CompleteCommand | K::ObservedCommandResult, _) => P::CommandWrite,
        _ => P::WriterApply,
    }
}

/// Storage refusal for a failure of `kind` at `phase`: an already closed
/// persistence error keeps its own diagnostics; any other failure is wrapped
/// in the kind's closed boundary with the phase, SQLite codes, elapsed wait
/// since `started` and one attempt, so every storage failure carries closed
/// diagnostics.
fn failure(
    kind: OpKindRef,
    phase: RejectStage,
    error: Error,
    started: Instant,
) -> StorageRejection {
    let error = match error {
        error @ Error::Persistence { .. } => error,
        source => Error::Persistence {
            operation: boundary(kind, phase),
            elapsed_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            attempts: 1,
            phase: Some(phase),
            source: Box::new(source),
        },
    };
    StorageRejection::from_error(&error, phase)
}

/// Ends a transaction that mutated nothing (replay or non-durable refusal):
/// the outcome is answered only after a confirmed rollback, otherwise the
/// outcome is unknown and the writer degrades.
fn release(conn: &Connection, result: SubmitResult) -> Executed {
    if rollback_confirmed(conn) {
        Executed::plain(result)
    } else {
        Executed {
            result: SubmitResult::Unknown(UnknownCause::RollbackUncertain),
            degraded: Some(Degradation::RollbackUncertain),
        }
    }
}

/// Ends the open transaction after a failure: a confirmed rollback yields the
/// given refusal, an unconfirmed one an unknown outcome plus degradation.
fn abort(conn: &Connection, refusal: StorageRejection) -> Executed {
    if rollback_confirmed(conn) {
        Executed::plain(SubmitResult::Rejected(refusal))
    } else {
        Executed {
            result: SubmitResult::Unknown(UnknownCause::RollbackUncertain),
            degraded: Some(Degradation::RollbackUncertain),
        }
    }
}

/// Encodes a value as canonical `ensure_ascii` JSON text.
fn canonical_text(value: &Value) -> String {
    String::from_utf8(dumps(value, true)).expect("ensure_ascii output is ASCII")
}

/// Runs one operation as one top-level transaction with its receipt.
///
/// `run` executes the decoded payload on `store` inside the open transaction
/// (store helpers nest savepoints, never commit) and returns its typed output
/// as JSON or a domain error. Domain errors become durable rejected receipts;
/// storage errors roll back the whole transaction. See the module docs for
/// the step order and outcome taxonomy.
pub(crate) fn execute(
    store: &mut Store,
    authority: &WriteAuthority,
    sub: Submission<'_>,
    run: impl FnOnce(&mut Store) -> Result<Value>,
) -> Executed {
    if sub.kind.internal_only() {
        return Executed::plain(refuse(
            RejectCode::Unsupported,
            RejectStage::Gate,
            "internal operation cannot be submitted alone",
        ));
    }
    if !sub.kind.permits(authority) {
        return Executed::plain(refuse(
            RejectCode::Unauthorized,
            RejectStage::Authorization,
            "authority may not submit this operation",
        ));
    }
    let started = Instant::now();
    if let Err(error) = store.conn.execute_batch("BEGIN IMMEDIATE") {
        let busy = matches!(
            error.sqlite_error_code(),
            Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
        );
        let mut refusal = StorageRejection::new(
            RejectCode::StorageUnavailable,
            RejectStage::Acquire,
            "write lock was not acquired",
        );
        refusal.persistence =
            failure(sub.kind, RejectStage::Acquire, Error::Sql(error), started).persistence;
        return Executed {
            result: SubmitResult::Rejected(refusal),
            degraded: busy.then_some(Degradation::UnknownWriter),
        };
    }
    let scope = if sub.kind.scoped() {
        sub.execution.clone()
    } else {
        None
    };
    // (refusal to report, or None for success) + receipt outcome/JSON.
    let (refusal, outcome, json) = match prepare(&store.conn, authority, &sub) {
        Err(error) => {
            return abort(
                &store.conn,
                failure(sub.kind, RejectStage::Statement, error, started),
            );
        }
        Ok(Prepared::Replay(result)) => return release(&store.conn, *result),
        Ok(Prepared::Refuse(code, stage, message)) => {
            return release(&store.conn, refuse(code, stage, message));
        }
        Ok(Prepared::Durable(code, message)) => {
            let mut refusal = StorageRejection::new(code, RejectStage::Authorization, message);
            refusal.durable = true;
            let json = canonical_text(&serde_json::to_value(&refusal).expect("refusal json"));
            (Some(refusal), "rejected", json)
        }
        Ok(Prepared::Run) => {
            if let Err(error) = store.conn.execute_batch("SAVEPOINT writer_op") {
                return abort(
                    &store.conn,
                    failure(sub.kind, RejectStage::Statement, Error::Sql(error), started),
                );
            }
            match run(store) {
                Ok(output) => {
                    if let Err(error) = store.conn.execute_batch("RELEASE writer_op") {
                        return abort(
                            &store.conn,
                            failure(sub.kind, RejectStage::Statement, Error::Sql(error), started),
                        );
                    }
                    let json = canonical_text(&output);
                    if json.len() > MAX_RESULT_BYTES {
                        return abort(
                            &store.conn,
                            StorageRejection::new(
                                RejectCode::PayloadTooLarge,
                                RejectStage::Statement,
                                "operation result exceeds its byte bound",
                            ),
                        );
                    }
                    (None, "committed", json)
                }
                Err(error) if storage_failure(&error) => {
                    return abort(
                        &store.conn,
                        failure(sub.kind, RejectStage::Statement, error, started),
                    );
                }
                Err(error) => {
                    if let Err(rollback) = store
                        .conn
                        .execute_batch("ROLLBACK TO writer_op; RELEASE writer_op")
                    {
                        return abort(
                            &store.conn,
                            failure(
                                sub.kind,
                                RejectStage::Statement,
                                Error::Sql(rollback),
                                started,
                            ),
                        );
                    }
                    let mut refusal = StorageRejection::from_error(&error, RejectStage::Statement);
                    refusal.durable = true;
                    let json =
                        canonical_text(&serde_json::to_value(&refusal).expect("refusal json"));
                    (Some(refusal), "rejected", json)
                }
            }
        }
    };
    let at = now();
    let seq = match insert_receipt(
        &store.conn,
        &sub,
        authority,
        scope.as_ref(),
        outcome,
        &json,
        at,
    ) {
        Ok(seq) => seq,
        Err(error) => {
            return abort(
                &store.conn,
                failure(sub.kind, RejectStage::Statement, error, started),
            );
        }
    };
    if let Err(error) = store.conn.execute_batch("COMMIT") {
        let busy = matches!(
            error.sqlite_error_code(),
            Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
        );
        if busy && !store.conn.is_autocommit() {
            let mut refusal = StorageRejection::new(
                RejectCode::StorageRolledBack,
                RejectStage::Commit,
                "commit failed and was rolled back",
            );
            refusal.persistence =
                failure(sub.kind, RejectStage::Commit, Error::Sql(error), started).persistence;
            return abort(&store.conn, refusal);
        }
        // Any other commit error may have committed: never claim a rollback,
        // but end a still-open transaction so none leaks into the next job.
        if !store.conn.is_autocommit() {
            let _ = store.conn.execute_batch("ROLLBACK");
        }
        return Executed {
            result: SubmitResult::Unknown(UnknownCause::CommitUncertain),
            degraded: Some(Degradation::CommitUncertain),
        };
    }
    let header = sub.header;
    Executed::plain(match refusal {
        Some(refusal) => SubmitResult::Rejected(refusal),
        None => SubmitResult::Committed(WriteReceipt {
            operation_id: header.operation_id.clone(),
            digest: header.digest.clone(),
            op_kind: sub.kind.name().to_owned(),
            producer: header.producer.clone(),
            ordinal: header.ordinal,
            scope,
            writer_epoch: sub.epoch,
            commit_seq: seq as u64,
            committed_at: at,
            replayed: false,
            result: serde_json::from_str(&json).expect("canonical json"),
        }),
    })
}

/// Event kind of recovery evidence about an original operation.
const RECOVERY_EVENT: &str = "recovery_observation_v1";

/// Records recovery evidence about an original operation under its own
/// identity (row 41). The execution must exist and own the attempt; an
/// original receipt scoped to another execution refuses. The original
/// receipt is only read (true for a committed receipt of the same digest,
/// false for a durable rejection of it, null when missing or for other
/// content) and never occupied, conflicted or replaced. The natural key is
/// the original operation id across all executions: a later record of the
/// same original replays the first stored record unchanged (another
/// execution claiming it refuses). The evidence is stored as
/// `recovered_unverified`, never as the original mutation or any proof.
pub(crate) fn record_recovery(
    conn: &Connection,
    op: &super::PersistRecoveryObservation,
) -> Result<super::RecoveryRecorded> {
    let invalid = agent_run_domain::error::invalid;
    if op.kind.is_empty()
        || op.kind.len() > 64
        || !op
            .kind
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(invalid(
            "recovery observation kind must be a closed snake_case label",
        ));
    }
    if dumps(&op.evidence, true).len() > 64 * 1024 || dumps(&op.result, true).len() > 4096 {
        return Err(invalid("recovery observation exceeds its byte bound"));
    }
    let owner: Option<String> = conn
        .query_row(
            "SELECT agent_id FROM attempts WHERE id=?",
            [&op.attempt],
            |row| row.get(0),
        )
        .optional()?;
    if owner.as_deref() != Some(op.execution.as_str()) {
        return Err(invalid("recovery attempt does not belong to the execution"));
    }
    let original = op.original_operation_id.as_str();
    let first: Option<(String, String)> = conn
        .query_row(
            "SELECT agent_id,data_json FROM events WHERE kind=? \
             AND json_extract(data_json,'$.original_operation_id')=? ORDER BY seq LIMIT 1",
            params![RECOVERY_EVENT, original],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((agent, data)) = first {
        if agent != op.execution.as_str() {
            return Err(Error::Conflict);
        }
        let data: Value = serde_json::from_str(&data)?;
        return Ok(super::RecoveryRecorded {
            recorded: false,
            original_committed: data["original_committed"].as_bool(),
        });
    }
    let row = stored(conn, &op.original_operation_id)?;
    if let Some(row) = &row
        && row
            .agent_id
            .as_deref()
            .is_some_and(|scope| scope != op.execution.as_str())
    {
        return Err(invalid("original receipt belongs to another execution"));
    }
    let original_committed = match row {
        Some(row) if row.digest == op.original_digest.as_str() => Some(row.outcome == "committed"),
        _ => None,
    };
    let data = serde_json::json!({
        "status": "recovered_unverified",
        "original_operation_id": original,
        "original_digest": op.original_digest.as_str(),
        "original_committed": original_committed,
        "kind": op.kind,
        "result": op.result,
        "evidence": op.evidence,
    });
    conn.execute(
        "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,?,?)",
        params![
            op.execution.as_str(),
            op.attempt,
            now(),
            RECOVERY_EVENT,
            canonical_text(&data)
        ],
    )?;
    Ok(super::RecoveryRecorded {
        recorded: true,
        original_committed,
    })
}

/// Receipt-page bound that leaves headroom for validation and writes within
/// the short cooperative budget under parallel broker load.
const RECEIPT_PRUNE_LIMIT: usize = 500;

/// Removes stale unscoped receipts and producer watermarks (row 87), at most
/// 500 receipts, or 2000 watermarks when no receipt candidates qualify. A receipt is removed only when it is unscoped, its commit is
/// older than 14 days of wall time AND its operation identity is stale on the
/// monotonic clock (another boot, or issued more than the maximum operation
/// age ago), so no live retry can need it; a wall-clock jump alone never
/// removes anything. Receipt pages follow commit time (sequence breaks ties),
/// using the age index so recent history is skipped under the producer gate.
/// A watermark is removed only when it scopes no
/// execution, no receipt references it, it is 14 days old in wall time, its
/// last issue is stale on the monotonic clock and its producer has no
/// unfinished scheduler work (`temp.writer_live_producers`, filled by the
/// sole writer in the same transaction). `boot_id`/`now_ns` must be
/// the writer's current boot and clock. Returns the number of removed rows.
/// `at` must be finite nonnegative Unix seconds. Read probes and bounded row
/// loops share a cooperative 200 ms budget. Read expiry is a storage failure;
/// deletion stops at the budget and returns the actual prefix for atomic commit.
/// Watermarks wait until an eligible receipt backlog drains; their potentially
/// expensive scan cannot consume the selected receipt page's mutation budget.
/// The sole writer owns this
/// connection's progress callback; it is cleared before primary-key mutations.
pub(crate) fn prune_receipts(conn: &Connection, op: &super::PruneReceipts) -> Result<usize> {
    prune_receipts_until(
        conn,
        op,
        Instant::now() + std::time::Duration::from_millis(200),
    )
}

/// Constructs the closed storage interruption for an exhausted read budget.
/// The caller has not changed persistent rows, so its normal rollback remains valid.
fn receipt_retention_interrupted() -> Error {
    Error::Sql(rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_INTERRUPT),
        None,
    ))
}

/// Selects before `deadline`, clears its SQL progress hook on every ordinary
/// return, then atomically prunes a budget-limited prefix in the caller's transaction.
/// Only read probes are interruptible: SQLite may auto-roll back interrupted DML.
fn prune_receipts_until(
    conn: &Connection,
    op: &super::PruneReceipts,
    deadline: Instant,
) -> Result<usize> {
    conn.execute(
        "CREATE TEMP TABLE IF NOT EXISTS writer_live_producers(key TEXT PRIMARY KEY)",
        [],
    )?;
    conn.progress_handler(
        1_000,
        Some(move || {
            #[cfg(test)]
            if READING_WATERMARKS.get() {
                WATERMARK_READ_PAUSE.with(|pause| {
                    if let Some(pause) = pause.borrow_mut().take() {
                        pause();
                    }
                });
            }
            Instant::now() >= deadline
        }),
    )?;
    let result = receipt_prune_candidates(conn, op, deadline);
    #[cfg(test)]
    READING_WATERMARKS.set(false);
    let cleanup = conn.progress_handler(0, None::<fn() -> bool>);
    let (receipts, watermarks) = result?;
    cleanup?;
    apply_receipt_pruning(conn, &receipts, &watermarks, || Instant::now() < deadline)
}

/// Reads at most 500 receipt sequences, or 2000 already-orphaned producer keys
/// under the caller's progress hook, with no persistent mutation. Validates the
/// same time/boot/retry guards as [`prune_receipts`]; `writer_live_producers` must
/// already exist. Read-budget expiry returns SQLITE_INTERRUPT before any deletes.
fn receipt_prune_candidates(
    conn: &Connection,
    op: &super::PruneReceipts,
    deadline: Instant,
) -> Result<(Vec<i64>, Vec<String>)> {
    let check_deadline = || {
        if Instant::now() >= deadline {
            Err(receipt_retention_interrupted())
        } else {
            Ok(())
        }
    };
    let invalid = agent_run_domain::error::invalid;
    if !op.at.is_finite() || op.at < 0.0 {
        return Err(invalid("receipt retention requires a finite Unix time"));
    }
    match agent_run_platform::monotonic::now() {
        Ok((boot, now)) if boot == op.boot_id && op.now_ns <= now => {}
        _ => {
            return Err(invalid(
                "receipt retention requires the current boot and clock",
            ));
        }
    }
    check_deadline()?;
    let cutoff = op.at - 14.0 * 86_400.0;
    let mut statement = conn.prepare(&format!(
        "SELECT commit_seq,operation_id FROM operation_receipts \
         WHERE agent_id IS NULL AND committed_at < ?1 ORDER BY committed_at,commit_seq LIMIT {RECEIPT_PRUNE_LIMIT}",
    ))?;
    let receipts: Vec<i64> = statement
        .query_map([cutoff], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .filter(|(_, id)| {
            OperationId::parse(id).is_ok_and(|id| id.is_stale(&op.boot_id, op.now_ns))
        })
        .map(|(seq, _)| seq)
        .collect();
    drop(statement);
    check_deadline()?;
    // ponytail: watermark scans wait for receipt backlog; split budgets if independent progress is needed.
    if !receipts.is_empty() {
        return Ok((receipts, Vec::new()));
    }
    let current = agent_run_domain::storage::boot_tag(&op.boot_id);
    #[cfg(test)]
    READING_WATERMARKS.set(true);
    // Producers with unfinished scheduler work (filled by the sole writer in
    // this transaction) keep their watermarks however old.
    let mut statement = conn.prepare(
        "SELECT w.producer_key,w.last_boot_tag,w.last_issued_ns FROM writer_producers w \
         WHERE w.updated_at < ?1 AND w.execution_id IS NULL \
         AND NOT EXISTS(SELECT 1 FROM operation_receipts r WHERE r.producer_key=w.producer_key) \
         AND w.producer_key NOT IN (SELECT key FROM temp.writer_live_producers) \
         ORDER BY w.producer_key LIMIT 2000",
    )?;
    let watermarks: Vec<String> = statement
        .query_map([cutoff], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .filter(|(_, tag, issued)| {
            *tag != current
                || op.now_ns.saturating_sub((*issued).max(0) as u64)
                    > agent_run_domain::storage::MAX_OPERATION_AGE_NS
        })
        .map(|(key, ..)| key)
        .collect();
    drop(statement);
    #[cfg(test)]
    READING_WATERMARKS.set(false);
    check_deadline()?;
    Ok((receipts, watermarks))
}

/// Deletes selected primary keys while `may_continue` permits another bounded
/// statement. The caller must clear interrupt hooks and own the transaction.
/// Returns the actual deleted prefix for atomic commit; zero progress on a
/// nonempty expired batch is a storage interruption, so maintenance retries.
/// Ordinary SQL errors propagate without a commit or a rollback claim here.
fn apply_receipt_pruning(
    conn: &Connection,
    receipts: &[i64],
    watermarks: &[String],
    mut may_continue: impl FnMut() -> bool,
) -> Result<usize> {
    let mut removed = 0;
    let expired = |removed| {
        if removed == 0 {
            Err(receipt_retention_interrupted())
        } else {
            Ok(removed)
        }
    };
    {
        let mut delete = conn.prepare("DELETE FROM operation_receipts WHERE commit_seq=?")?;
        for seq in receipts {
            if !may_continue() {
                return expired(removed);
            }
            removed += delete.execute([seq])?;
        }
    }
    {
        let mut delete = conn.prepare("DELETE FROM writer_producers WHERE producer_key=?")?;
        for key in watermarks {
            if !may_continue() {
                return expired(removed);
            }
            removed += delete.execute([key])?;
        }
    }
    Ok(removed)
}

/// Stored outcome of `id` regardless of authority (trusted read path).
pub(crate) fn stored_outcome(conn: &Connection, id: &OperationId) -> Result<Option<SubmitResult>> {
    stored(conn, id)?
        .map(|row| replay(id, row, true))
        .transpose()
}

#[cfg(test)]
mod tests {
    //! Rollback confirmation and closed persistence diagnostics of the
    //! transaction boundary, checked without a writer home.
    use super::*;

    /// Retention must leave recent receipts intact without spending work proportional
    /// to their population while the queue's producer-admission gate is held.
    /// A deterministic SQLite VM budget catches a full history scan even on a fast disk;
    /// one expired receipt and ten stale orphan watermarks prove both indexed
    /// selectors perform real work. The batch seam preserves the counting hook.
    #[test]
    fn receipt_pruning_skips_large_recent_history_with_bounded_work() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../../sql/schema.sql"))
            .unwrap();
        let (boot, now_ns) = agent_run_platform::monotonic::now().unwrap();
        let at = agent_run_domain::domain::now();
        let tx = conn.transaction().unwrap();
        {
            let mut insert = tx.prepare(
                "INSERT INTO operation_receipts(operation_id,digest,op_kind,producer_key,ordinal,authority_kind,authority_fingerprint,agent_id,outcome,result_json,writer_epoch,committed_at) \
                 VALUES(?1,?2,'fixture','retention-fixture',?3,'broker',?4,NULL,'committed','{}',1,?5)",
            ).unwrap();
            for ordinal in 1..=20_000 {
                insert
                    .execute(params![
                        OperationId::new("retention-fixture", ordinal).as_str(),
                        "d".repeat(64),
                        ordinal,
                        "f".repeat(64),
                        at,
                    ])
                    .unwrap();
            }
            insert
                .execute(params![
                    OperationId::new("expired-fixture", 1).as_str(),
                    "d".repeat(64),
                    20_001,
                    "f".repeat(64),
                    at - 15.0 * 86_400.0,
                ])
                .unwrap();
        }
        tx.commit().unwrap();
        for ordinal in 0..10 {
            conn.execute(
                "INSERT INTO writer_producers VALUES(?1,1,?2,0,NULL,?3)",
                params![
                    format!("retention-orphan-{ordinal:03}"),
                    if agent_run_domain::storage::boot_tag(&boot) == "0000000000000000" {
                        "ffffffffffffffff"
                    } else {
                        "0000000000000000"
                    },
                    at - 15.0 * 86_400.0,
                ],
            )
            .unwrap();
        }
        conn.execute(
            "CREATE TEMP TABLE writer_live_producers(key TEXT PRIMARY KEY)",
            [],
        )
        .unwrap();
        let steps = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter = steps.clone();
        conn.progress_handler(
            1,
            Some(move || {
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                false
            }),
        )
        .unwrap();
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let op = super::super::PruneReceipts {
            at,
            boot_id: boot,
            now_ns,
        };
        let (receipts, watermarks) = receipt_prune_candidates(&conn, &op, deadline).unwrap();
        let removed =
            apply_receipt_pruning(&conn, &receipts, &watermarks, || Instant::now() < deadline)
                .unwrap();
        assert_eq!(
            removed, 1,
            "receipt page gets its mutation budget before watermark scanning"
        );
        let (receipts, watermarks) = receipt_prune_candidates(&conn, &op, deadline).unwrap();
        let removed = removed
            + apply_receipt_pruning(&conn, &receipts, &watermarks, || Instant::now() < deadline)
                .unwrap();
        conn.progress_handler(0, None::<fn() -> bool>).unwrap();
        let steps = steps.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(removed, 11);
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM operation_receipts", [], |row| row
                .get::<_, u64>(0))
                .unwrap(),
            20_000
        );
        assert!(
            steps > 0 && steps < 5_000,
            "receipt retention spent {steps} VM steps over recent history"
        );
    }

    /// Expiry after a real deletion preserves that prefix for one atomic commit
    /// and leaves the connection usable. A deterministic budget predicate avoids
    /// timing guesses or relaxing the production deadline.
    #[test]
    fn receipt_pruning_commits_progress_when_budget_ends_between_rows() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../../sql/schema.sql"))
            .unwrap();
        for ordinal in 1..=2 {
            conn.execute(
                "INSERT INTO operation_receipts(operation_id,digest,op_kind,producer_key,ordinal,authority_kind,authority_fingerprint,agent_id,outcome,result_json,writer_epoch,committed_at) VALUES(?1,?2,'fixture','budget-fixture',?3,'broker',?4,NULL,'committed','{}',1,0)",
                params![OperationId::new("budget-fixture", ordinal).as_str(), "d".repeat(64), ordinal, "f".repeat(64)],
            ).unwrap();
        }
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut checks = 0;
        let removed = apply_receipt_pruning(&conn, &[1, 2], &[], || {
            checks += 1;
            checks == 1
        })
        .unwrap();
        assert_eq!(removed, 1);
        assert!(!conn.is_autocommit());
        conn.execute_batch("COMMIT").unwrap();
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM operation_receipts", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        conn.execute("DELETE FROM operation_receipts", []).unwrap();
        assert!(conn.is_autocommit());
    }

    /// Expired retention returns a storage interruption and clears its callback,
    /// allowing the caller to roll back and execute later substantial SQL normally.
    #[test]
    fn receipt_pruning_deadline_interrupt_clears_callback() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../../sql/schema.sql"))
            .unwrap();
        let (boot_id, now_ns) = agent_run_platform::monotonic::now().unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let error = prune_receipts_until(
            &conn,
            &super::super::PruneReceipts {
                at: agent_run_domain::domain::now(),
                boot_id,
                now_ns,
            },
            Instant::now(),
        )
        .unwrap_err();
        assert!(
            matches!(error, Error::Sql(error) if error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted))
        );
        conn.execute_batch("ROLLBACK").unwrap();
        let sum: i64 = conn
            .query_row(
                "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT SUM(x) FROM n",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sum, 50_005_000);
    }

    /// A SQLite failure with `code` as its extended result code.
    fn sql(code: i32) -> Error {
        Error::Sql(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(code),
            None,
        ))
    }

    /// A replay or non-durable refusal whose rollback cannot be confirmed is
    /// unknown and degrades the writer; a confirmed rollback answers the
    /// outcome and leaves no transaction open.
    #[test]
    fn release_requires_a_confirmed_rollback() {
        let conn = Connection::open_in_memory().unwrap();
        let answer = refuse(RejectCode::OrderViolation, RejectStage::Gate, "order");
        let unconfirmed = release(&conn, answer.clone());
        assert!(matches!(
            unconfirmed.result,
            SubmitResult::Unknown(UnknownCause::RollbackUncertain)
        ));
        assert_eq!(unconfirmed.degraded, Some(Degradation::RollbackUncertain));
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let confirmed = release(&conn, answer);
        assert!(matches!(confirmed.result, SubmitResult::Rejected(_)));
        assert_eq!(confirmed.degraded, None);
        assert!(conn.is_autocommit());
    }

    /// Writer failures carry constructed closed diagnostics (operation,
    /// phase, primary and extended codes, wait and one attempt); only an
    /// ownership acquire stays retryable, a commit-phase failure never is,
    /// and kinds without a legacy label carry the writer fallback.
    #[test]
    fn failures_construct_closed_persistence_diagnostics() {
        let started = Instant::now() - std::time::Duration::from_millis(30);
        let acquire = failure(
            OpKindRef::RememberProcesses,
            RejectStage::Acquire,
            sql(517),
            started,
        );
        let diag = acquire.persistence.clone().unwrap();
        assert_eq!(diag.operation, PersistenceOperation::OwnershipAcquire);
        assert_eq!(diag.phase, RejectStage::Acquire);
        assert_eq!(
            (diag.sqlite_primary, diag.sqlite_extended),
            (Some(5), Some(517))
        );
        assert!(diag.wait_ms >= 30);
        assert_eq!(diag.attempts, 1);
        assert!(acquire.into_error().retryable_ownership_acquire());
        let commit = failure(
            OpKindRef::RememberProcesses,
            RejectStage::Commit,
            sql(5),
            started,
        );
        assert_eq!(
            commit.persistence.as_ref().unwrap().operation,
            PersistenceOperation::OwnershipCommit
        );
        assert!(!commit.into_error().retryable_ownership_acquire());
        let terminal = failure(
            OpKindRef::FinishTerminal,
            RejectStage::Statement,
            sql(19),
            started,
        );
        let diag = terminal.persistence.unwrap();
        assert_eq!(diag.operation, PersistenceOperation::TerminalCommit);
        assert_eq!(diag.phase, RejectStage::Statement);
        assert_eq!(diag.sqlite_extended, Some(19));
        let preserved = Error::Persistence {
            operation: PersistenceOperation::OwnershipWrite,
            elapsed_ms: 7,
            attempts: 3,
            phase: Some(RejectStage::Statement),
            source: Box::new(sql(5)),
        };
        let kept = failure(
            OpKindRef::RememberProcesses,
            RejectStage::Statement,
            preserved,
            started,
        );
        assert_eq!(kept.persistence.as_ref().unwrap().attempts, 3);
        assert_eq!(kept.persistence.unwrap().wait_ms, 7);
        let fallback = failure(OpKindRef::BindPool, RejectStage::Acquire, sql(5), started);
        assert_eq!(
            fallback.persistence.as_ref().unwrap().operation,
            PersistenceOperation::WriterApply
        );
        assert!(!fallback.into_error().retryable_ownership_acquire());
    }

    /// Every implemented row at every writer phase carries closed
    /// diagnostics with that phase, the SQLite codes, a wait and one attempt;
    /// only the ownership acquire of process snapshots is replayable, and
    /// rows without a legacy label use the writer fallback, never an
    /// ownership label.
    #[test]
    fn every_implemented_kind_and_phase_carries_diagnostics() {
        let started = Instant::now();
        let mut fallback = 0;
        for kind in OpKindRef::all()
            .iter()
            .copied()
            .filter(|kind| super::super::implemented(*kind))
        {
            for phase in [
                RejectStage::Acquire,
                RejectStage::Statement,
                RejectStage::Commit,
            ] {
                let refusal = failure(kind, phase, sql(517), started);
                let diag = refusal
                    .persistence
                    .clone()
                    .unwrap_or_else(|| panic!("{kind:?} {phase:?}"));
                assert_eq!(diag.phase, phase, "{kind:?}");
                assert_eq!(
                    (diag.sqlite_primary, diag.sqlite_extended),
                    (Some(5), Some(517))
                );
                assert_eq!(diag.attempts, 1);
                let ownership = matches!(
                    diag.operation,
                    PersistenceOperation::OwnershipAcquire
                        | PersistenceOperation::OwnershipWrite
                        | PersistenceOperation::OwnershipCommit
                );
                assert_eq!(ownership, kind == OpKindRef::RememberProcesses, "{kind:?}");
                if diag.operation == PersistenceOperation::WriterApply {
                    fallback += 1;
                }
                assert_eq!(
                    refusal.into_error().retryable_ownership_acquire(),
                    kind == OpKindRef::RememberProcesses && phase == RejectStage::Acquire,
                    "{kind:?} {phase:?}"
                );
            }
        }
        assert!(fallback > 0);
    }
}
