//! Bounded, content-free incident observations independent of expirable history.
//! Capture is best effort after lifecycle commits; retention requires a committed
//! projection before destroying its source. No prompt, answer, argument, account,
//! native session, path, free-form error or delivery message identifier is copied.

use crate::ops::{IncidentPhase, IncidentProjection};
use agent_run_domain::{MachineCode, Result, canonical, domain::AgentId, error::invalid};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Map, Value, json};

/// Compact incident observations expire after thirty days, independently of
/// ordinary session-count retirement. Unix-time inputs must be finite.
pub const HISTORY_SECONDS: f64 = 30.0 * 86400.0;
/// Maximum retained phase records; immutable execution/phase keys deduplicate
/// retries. Oldest records are removed in bounded maintenance batches.
pub const MAX_RECORDS: i64 = 10_000;
/// Maximum UTF-8 JSON bytes in one closed diagnostic projection.
const MAX_BYTES: usize = 4096;
/// Hard ledger bound: rows of executions whose sources still exist are never
/// evicted, so the cap may be exceeded up to here; beyond it new rows are
/// refused (a history candidate is then deferred) rather than evicting
/// evidence still needed.
pub const HARD_MAX_RECORDS: i64 = 2 * MAX_RECORDS;
/// Rows that age or cap pruning may evict: only executions whose agents row
/// (and so whose source) is gone.
const EVICTABLE: &str = "execution_id NOT IN (SELECT id FROM agents)";

/// The only ledger phase refreshed in place: the source proof of the latest
/// projection of its execution.
pub const PROOF_PHASE: &str = "projection_proof";
/// Exact version of the proof details.
const PROOF_VERSION: u64 = 2;

/// Builds the bounded incident projection of one execution from its durable
/// rows without writing (`conn` may be a reader; callers needing one
/// consistent snapshot wrap it in a read transaction). `id` is validated; `at`
/// is a finite nonnegative observation time. Returns `None` for an unknown
/// execution. Execution failures keep only closed fields: class, stage,
/// ownership category, SQLite/OS codes, the persistence operation (closed
/// wire name, else `unknown`) and writer phase (closed label, else null),
/// wait milliseconds (0..=86 400 000) and attempts (0..=1 000 000);
/// out-of-range or malformed numbers become null. Valid typed writer operation
/// ids and closed inventory names are retained when present; missing historical
/// fields remain absent so old immutable phase projections stay identical.
///
/// The last phase is always the [`PROOF_PHASE`] observed at `at`, with details
/// `{"version":2,"fingerprint":…,"evidence":{phase: details}}`. The fingerprint
/// is the canonical SHA-256 of the bounded source: agent status, finish time
/// and exit code; the first execution failure and the latest cleanup event
/// (sequence, time, closed values); the first two terminal deliveries by
/// terminal event then id (id, state, attempts, ambiguity; two in one state
/// share that state's phase, the second's values in closed fields); absences are
/// explicit and the observation time is excluded. The evidence is the
/// stored proof's evidence overlaid with this projection's diagnostic
/// phases, so evidence whose source an earlier purge removed survives. A
/// success without an execution fault has no diagnostic phase and an
/// empty-evidence proof, which is not a failure.
pub fn project(conn: &Connection, id: &str, at: f64) -> Result<Option<IncidentProjection>> {
    let execution: AgentId = id.parse()?;
    if !at.is_finite() || at < 0.0 {
        return Err(invalid("incident time must be finite and nonnegative"));
    }
    let row: Option<(String, Option<f64>, Option<i32>)> = conn
        .query_row(
            "SELECT status,finished_at,exit_code FROM agents WHERE id=?",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((status, finished, exit)) = row else {
        return Ok(None);
    };
    let event = |order: &str, kind: &str| -> Result<Option<(i64, f64, Value)>> {
        let row: Option<(i64, f64, String)> = conn
            .query_row(
                &format!("SELECT seq,at,data_json FROM events WHERE agent_id=? AND kind=? ORDER BY seq {order} LIMIT 1"),
                params![id, kind],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        Ok(row
            .map(|(seq, when, raw)| (seq, when, serde_json::from_str(&raw).unwrap_or(Value::Null))))
    };
    let failure = event("ASC", "execution_failure_v1")?
        .map(|(seq, when, value)| (seq, when, failure_details(&value)));
    let cleanup = event("DESC", "process_cleanup")?.map(|(seq, when, v)| {
        (
            seq,
            when,
            json!({"version":1,"confirmed":v["confirmed"].as_bool(),"group_gone":v["group_gone"].as_bool(),"descendants_gone":v["descendants_gone"].as_bool()}),
        )
    });
    let mut stmt = conn.prepare(
        "SELECT id,state,attempts,ambiguous_result FROM deliveries \
         WHERE agent_id=? AND terminal_event_seq IS NOT NULL ORDER BY terminal_event_seq,id LIMIT 2",
    )?;
    let deliveries = stmt
        .query_map([id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, bool>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);
    let fingerprint = canonical::sha256_hex(
        &json!({
            "agent": [status, finished, exit],
            "failure": failure.as_ref().map(|(seq, when, details)| json!([seq, when, details])),
            "cleanup": cleanup.as_ref().map(|(seq, when, details)| json!([seq, when, details])),
            "deliveries": deliveries
                .iter()
                .map(|(id, state, attempts, ambiguous)| json!([id, state, attempts, ambiguous]))
                .collect::<Vec<_>>(),
        }),
        true,
    );
    let mut phases = Vec::new();
    let failed = matches!(
        status.as_str(),
        "failed" | "timed_out" | "cancelled" | "lost"
    );
    if failed || failure.is_some() {
        if failed {
            phases.push(IncidentPhase {
                phase: "terminal".into(),
                occurred_at: finished
                    .filter(|v| v.is_finite() && *v >= 0.0)
                    .unwrap_or(at),
                details: json!({"version":1,"status":status,"exit_code":exit,"finished_at":finished}),
            });
        }
        for (phase, event) in [("execution_failure", failure), ("cleanup", cleanup)] {
            if let Some((_, when, details)) = event {
                phases.push(IncidentPhase {
                    phase: phase.into(),
                    occurred_at: when,
                    details,
                });
            }
        }
        // One phase per delivery state: a second selected delivery in the
        // same state keeps its own attempts and ambiguity in closed fields.
        for (_, state, attempts, ambiguous) in deliveries {
            if !DELIVERY_STATES.contains(&state.as_str()) {
                continue;
            }
            let phase = format!("delivery_{state}");
            match phases.iter_mut().find(|existing| existing.phase == phase) {
                Some(existing) => {
                    existing.details["deliveries"] = json!(2);
                    existing.details["second_attempts"] = json!(attempts);
                    existing.details["second_ambiguous"] = json!(ambiguous);
                }
                None => phases.push(IncidentPhase {
                    phase,
                    occurred_at: at,
                    details: json!({"version":1,"state":state,"attempts":attempts,"ambiguous":ambiguous}),
                }),
            }
        }
    }
    let stored: Option<String> = conn
        .query_row(
            "SELECT details_json FROM incident_ledger WHERE execution_id=? AND phase=?",
            params![id, PROOF_PHASE],
            |r| r.get(0),
        )
        .optional()?;
    let mut evidence = match stored {
        Some(raw) => proof_evidence(&serde_json::from_str(&raw)?)?.clone(),
        None => Map::new(),
    };
    for phase in &phases {
        evidence.insert(phase.phase.clone(), phase.details.clone());
    }
    phases.push(IncidentPhase {
        phase: PROOF_PHASE.into(),
        occurred_at: at,
        details: json!({"version":PROOF_VERSION,"fingerprint":fingerprint,"evidence":evidence}),
    });
    Ok(Some(IncidentProjection { execution, phases }))
}

/// Closed details of one `execution_failure_v1` event value.
fn failure_details(value: &Value) -> Value {
    let stage = value["stage"]
        .as_str()
        .filter(|s| {
            matches!(
                *s,
                "engine_execution" | "initial" | "streaming" | "final" | "observation"
            )
        })
        .unwrap_or("unknown");
    let class = value["class"]
        .as_str()
        .and_then(MachineCode::from_wire)
        .map(MachineCode::as_str)
        .unwrap_or("unknown");
    let category = value["ownership_category"]
        .as_str()
        .filter(|s| {
            matches!(
                *s,
                "integrity_failure" | "persistence_sqlite" | "io_failure" | "checkpoint_failure"
            )
        })
        .unwrap_or("unknown");
    let operation = serde_json::from_value::<agent_run_domain::error::PersistenceOperation>(
        value["operation"].clone(),
    )
    .ok()
    .and_then(|operation| serde_json::to_value(operation).ok())
    .unwrap_or_else(|| json!("unknown"));
    // The closed writer phase survives only as a closed stage label;
    // legacy, malformed or free-form values become null.
    let phase =
        serde_json::from_value::<agent_run_domain::storage::RejectStage>(value["phase"].clone())
            .ok()
            .and_then(|phase| serde_json::to_value(phase).ok())
            .unwrap_or(Value::Null);
    let mut safe = json!({"version":1,"class":class,"stage":stage,"ownership_category":category,"operation":operation,"phase":phase});
    // Add only new, strictly validated fields. Missing historical fields do
    // not produce new null keys, preserving immutable old phase projections.
    if let Some(id) = value["writer_operation_id"]
        .as_str()
        .and_then(|id| agent_run_domain::storage::OperationId::parse(id).ok())
    {
        safe["writer_operation_id"] = json!(id);
    }
    if let Some(kind) = value["writer_op_kind"].as_str().filter(|name| {
        crate::ops::OpKindRef::all()
            .iter()
            .any(|kind| kind.name() == *name)
    }) {
        safe["writer_op_kind"] = json!(kind);
    }
    for key in ["sqlite_extended_code", "os_code"] {
        safe[key] = value[key]
            .as_i64()
            .filter(|n| i32::try_from(*n).is_ok())
            .map(Value::from)
            .unwrap_or(Value::Null);
    }
    safe["wait_ms"] = value["wait_ms"]
        .as_u64()
        .filter(|n| *n <= 86_400_000)
        .map(Value::from)
        .unwrap_or(Value::Null);
    safe["attempts"] = value["attempts"]
        .as_u64()
        .filter(|n| *n <= 1_000_000)
        .map(Value::from)
        .unwrap_or(Value::Null);
    safe
}

/// Delivery states copied into the ledger.
const DELIVERY_STATES: [&str; 8] = [
    "waiting_binding",
    "pending",
    "sending",
    "delivered",
    "retry_wait",
    "failed",
    "cancelled",
    "expired",
];

/// Whether `phase` is one of the closed diagnostic (immutable) phase names.
fn known_phase(phase: &str) -> bool {
    matches!(phase, "terminal" | "execution_failure" | "cleanup")
        || phase
            .strip_prefix("delivery_")
            .is_some_and(|state| DELIVERY_STATES.contains(&state))
}

/// The evidence of well-formed proof details: exactly `version` (the integer
/// 2), a lowercase 64-hex `fingerprint` and an `evidence` object mapping
/// closed diagnostic phase names to objects. Anything else refuses.
fn proof_evidence(details: &Value) -> Result<&Map<String, Value>> {
    let refuse = || invalid("incident projection proof is malformed");
    let fields = details.as_object().ok_or_else(refuse)?;
    let fingerprint = fields
        .get("fingerprint")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if fields.len() != 3
        || fields.get("version").and_then(Value::as_u64) != Some(PROOF_VERSION)
        || fingerprint.len() != 64
        || !fingerprint
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(refuse());
    }
    let evidence = fields
        .get("evidence")
        .and_then(Value::as_object)
        .ok_or_else(refuse)?;
    if evidence
        .iter()
        .any(|(phase, details)| !known_phase(phase) || !details.is_object())
    {
        return Err(refuse());
    }
    Ok(evidence)
}

/// Validates a projection before any mutation and returns its encoded rows:
/// at most 8 phases with distinct closed names, object details of at most
/// 4096 bytes, valid clocks, and exactly one well-formed proof whose evidence
/// holds each diagnostic phase's details.
pub(crate) fn validate(projection: &IncidentProjection) -> Result<Vec<(&str, f64, String)>> {
    if projection.phases.len() > 8 {
        return Err(invalid("incident projection has too many phases"));
    }
    let mut rows: Vec<(&str, f64, String)> = Vec::with_capacity(projection.phases.len());
    let mut proof = None;
    for phase in &projection.phases {
        let name = phase.phase.as_str();
        if !(known_phase(name) || name == PROOF_PHASE) || !phase.details.is_object() {
            return Err(invalid("incident phase is not closed"));
        }
        if rows.iter().any(|(seen, _, _)| *seen == name) {
            return Err(invalid("incident phase is duplicated"));
        }
        if !phase.occurred_at.is_finite() || phase.occurred_at < 0.0 {
            return Err(invalid("incident phase time is invalid"));
        }
        let bytes = serde_json::to_string(&phase.details)?;
        if bytes.len() > MAX_BYTES {
            return Err(invalid("incident projection exceeds four KiB"));
        }
        if name == PROOF_PHASE {
            proof = Some(proof_evidence(&phase.details)?);
        }
        rows.push((name, phase.occurred_at, bytes));
    }
    let evidence = proof.ok_or_else(|| invalid("incident projection has no source proof"))?;
    if projection.phases.iter().any(|phase| {
        phase.phase != PROOF_PHASE && evidence.get(&phase.phase) != Some(&phase.details)
    }) {
        return Err(invalid("incident projection proof is inconsistent"));
    }
    Ok(rows)
}

/// Whether `projection` is well formed (else it refuses) and still exactly
/// what its source yields at its proof's observation time: the rows 85/91
/// recheck inside the writer transaction.
pub(crate) fn current(conn: &Connection, projection: &IncidentProjection) -> Result<bool> {
    validate(projection)?;
    let at = projection
        .phases
        .iter()
        .find(|phase| phase.phase == PROOF_PHASE)
        .map_or(0.0, |phase| phase.occurred_at);
    Ok(project(conn, projection.execution.as_str(), at)?.as_ref() == Some(projection))
}

/// Stores a validated projection without the normal record-cap pruning
/// (see [`persist`] and [`prune`]) but within [`HARD_MAX_RECORDS`], which it
/// enforces before any write: diagnostic phases are immutable (an existing
/// execution/phase keeps its first evidence) and the proof is refreshed on
/// its own key. Returns the number of inserted or changed rows.
pub(crate) fn store(conn: &Connection, projection: &IncidentProjection) -> Result<usize> {
    let rows = validate(projection)?;
    if !fits(conn, projection)? {
        return Err(invalid("incident ledger is at its hard bound"));
    }
    let mut changed = 0;
    for (phase, at, bytes) in rows {
        changed += conn.execute(
            "INSERT INTO incident_ledger(execution_id,phase,occurred_at,details_json) VALUES(?1,?2,?3,?4) \
             ON CONFLICT(execution_id,phase) DO UPDATE SET occurred_at=excluded.occurred_at,details_json=excluded.details_json \
             WHERE phase='projection_proof' AND (occurred_at IS NOT excluded.occurred_at OR details_json IS NOT excluded.details_json)",
            params![projection.execution.as_str(), phase, at, bytes],
        )?;
    }
    Ok(changed)
}

/// Whether storing `projection` keeps the ledger within
/// [`HARD_MAX_RECORDS`]: only phases not yet stored add rows (a proof
/// refresh does not).
pub(crate) fn fits(conn: &Connection, projection: &IncidentProjection) -> Result<bool> {
    let phases: Vec<&str> = projection.phases.iter().map(|p| p.phase.as_str()).collect();
    let (total, stored): (i64, i64) = conn.query_row(
        "SELECT (SELECT COUNT(*) FROM incident_ledger), \
         (SELECT COUNT(*) FROM incident_ledger WHERE execution_id=?1 AND phase IN (SELECT value FROM json_each(?2)))",
        params![projection.execution.as_str(), serde_json::to_string(&phases)?],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(total + phases.len() as i64 - stored <= HARD_MAX_RECORDS)
}

/// Whether the durable ledger holds `projection`'s proof exactly and a row
/// for each of its diagnostic phases (a first evidence kept by an earlier
/// insertion counts; an attempted insertion does not).
pub(crate) fn covered(conn: &Connection, projection: &IncidentProjection) -> Result<bool> {
    for phase in &projection.phases {
        let stored: Option<(f64, String)> = conn
            .query_row(
                "SELECT occurred_at,details_json FROM incident_ledger WHERE execution_id=? AND phase=?",
                params![projection.execution.as_str(), phase.phase],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let holds = match stored {
            None => false,
            Some(_) if phase.phase != PROOF_PHASE => true,
            Some((at, raw)) => {
                at == phase.occurred_at && serde_json::from_str::<Value>(&raw)? == phase.details
            }
        };
        if !holds {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Persists a projection using the internal `store` helper on `conn`, then enforces the
/// record cap over executions whose sources are gone. Malformed projections
/// and a full ledger refuse before mutation. Returns the number of inserted
/// or changed rows.
pub fn persist(conn: &Connection, projection: &IncidentProjection) -> Result<usize> {
    let changed = store(conn, projection)?;
    // The independent ledger has a hard record cap even when ordinary
    // retirement is idle; no old agent/transcript is deleted to enforce it.
    conn.execute(
        &format!(
            "DELETE FROM incident_ledger WHERE rowid IN (SELECT rowid FROM incident_ledger \
             WHERE {EVICTABLE} ORDER BY occurred_at DESC,execution_id DESC,phase DESC LIMIT -1 OFFSET ?1)"
        ),
        [MAX_RECORDS],
    )?;
    Ok(changed)
}

/// Copies an already terminal incident, its known diagnostic phases and its
/// source proof using `conn`: [`project`] then [`persist`]. Source errors
/// propagate so callers can defer rather than erase uncaptured evidence.
pub fn capture(conn: &Connection, id: &str, at: f64) -> Result<()> {
    if let Some(projection) = project(conn, id, at)? {
        persist(conn, &projection)?;
    }
    Ok(())
}

/// Read-only eligibility probe for independent ledger age/count expiry. An idle
/// store must not acquire a writer merely to discover that nothing expires.
pub fn prune_pending(conn: &Connection, at: f64) -> Result<bool> {
    if !at.is_finite() || at < 0.0 {
        return Err(invalid("incident retention time is invalid"));
    }
    Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM incident_ledger WHERE occurred_at < ?1 LIMIT 1) OR (SELECT COUNT(*) FROM incident_ledger) > ?2", params![at - HISTORY_SECONDS, MAX_RECORDS], |row| row.get(0))?)
}

/// Expires one bounded ledger batch at finite Unix time `at`. The caller owns
/// its short maintenance transaction/progress budget; no agent row is removed.
/// Oldest overflow and age-expired phases use the same deterministic order.
/// Rows of an execution whose agents row still exists are never evicted:
/// a partially purged history keeps its first and latest evidence until its
/// source is gone, and normal age/cap retention applies afterwards.
pub fn prune(conn: &Connection, at: f64) -> Result<usize> {
    if !at.is_finite() || at < 0.0 {
        return Err(invalid("incident retention time is invalid"));
    }
    Ok(conn.execute(
        &format!(
            "DELETE FROM incident_ledger WHERE rowid IN (SELECT rowid FROM incident_ledger \
             WHERE occurred_at < ?1 AND {EVICTABLE} ORDER BY occurred_at,execution_id,phase LIMIT 2000)"
        ),
        [at - HISTORY_SECONDS],
    )? + conn.execute(
        &format!(
            "DELETE FROM incident_ledger WHERE rowid IN (SELECT rowid FROM incident_ledger \
             WHERE {EVICTABLE} ORDER BY occurred_at DESC,execution_id DESC,phase DESC LIMIT 2000 OFFSET ?1)"
        ),
        [MAX_RECORDS],
    )?)
}

/// Opens an existing current ledger read-only with a 100 ms lock allowance and
/// cooperative 200 ms query budget, without migration or model/config access.
/// WAL-aware SQLite reads preserve fresh observations; failures remain explicit.
pub fn read_summary(path: &std::path::Path) -> Result<Value> {
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(std::time::Duration::from_millis(100))?;
    conn.pragma_update(None, "query_only", true)?;
    let version: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if version != crate::VERSION {
        return Err(invalid("incident ledger requires the current state schema"));
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
    conn.progress_handler(1000, Some(move || std::time::Instant::now() >= deadline))?;
    let result = summary(&conn);
    conn.progress_handler(0, None::<fn() -> bool>)?;
    result
}

/// Returns aggregate-only diagnostics from a current connection. No stored
/// JSON, identifier or source string enters the result; absent evidence stays
/// absent. Work is bounded by the ledger's own row cap/maintenance policy.
pub fn summary(conn: &Connection) -> Result<Value> {
    let (count, oldest, newest): (i64, Option<f64>, Option<f64>) = conn.query_row(
        "SELECT COUNT(*),MIN(occurred_at),MAX(occurred_at) FROM incident_ledger",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    Ok(
        json!({"records":count,"oldest_at":oldest,"newest_at":newest,"retention_seconds":HISTORY_SECONDS,"max_records":MAX_RECORDS}),
    )
}

impl crate::Store<'_> {
    /// Persists a reader-computed incident projection best-effort after the
    /// terminal commit (writer unit of `CaptureIncident`). The execution must
    /// exist and be terminal; a malformed projection refuses and one that no
    /// longer equals what its source yields in this transaction is a
    /// `Conflict`. The stored proof and diagnostics are verified durable
    /// before commit. Returns whether any row was inserted or refreshed.
    pub fn capture_incident(&mut self, projection: &IncidentProjection) -> Result<bool> {
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let status: Option<String> = tx
            .query_row(
                "SELECT status FROM agents WHERE id=?",
                [projection.execution.as_str()],
                |r| r.get(0),
            )
            .optional()?;
        let Some(status) = status else {
            return Err(agent_run_domain::Error::NotFound(
                projection.execution.to_string(),
            ));
        };
        if !matches!(
            status.as_str(),
            "succeeded" | "failed" | "cancelled" | "lost" | "timed_out"
        ) {
            return Err(agent_run_domain::Error::Transition(
                "incident capture requires a terminal agent".into(),
            ));
        }
        if !current(&tx, projection)? {
            return Err(agent_run_domain::Error::Conflict);
        }
        let changed = persist(&tx, projection)?;
        if !covered(&tx, projection)? {
            return Err(agent_run_domain::Error::Integrity(
                "incident projection is not durable".into(),
            ));
        }
        tx.commit()?;
        Ok(changed > 0)
    }
}
