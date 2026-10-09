//! Bounded, content-free incident observations independent of expirable history.
//! Capture is best effort after lifecycle commits; retention requires a committed
//! projection before destroying its source. No prompt, answer, argument, account,
//! native session, path, free-form error or delivery message identifier is copied.

use agent_run_domain::{MachineCode, Result, domain::AgentId, error::invalid};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

/// Compact incident observations expire after thirty days, independently of
/// ordinary session-count retirement. Unix-time inputs must be finite.
pub const HISTORY_SECONDS: f64 = 30.0 * 86400.0;
/// Maximum retained phase records; immutable execution/phase keys deduplicate
/// retries. Oldest records are removed in bounded maintenance batches.
pub const MAX_RECORDS: i64 = 10_000;
/// Maximum UTF-8 JSON bytes in one closed diagnostic projection.
const MAX_BYTES: usize = 4096;

/// Copies an already terminal incident and its known diagnostic phases using
/// `conn` (which may be the caller's retention transaction). `id` is validated;
/// `at` is a finite nonnegative observation time. Successes without an execution
/// fault are omitted. Inserts are immutable/idempotent; source errors propagate
/// so retention can defer rather than erase uncaptured incident evidence.
pub fn capture(conn: &Connection, id: &str, at: f64) -> Result<()> {
    let _: AgentId = id.parse()?;
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
        return Ok(());
    };
    let failure: Option<(f64,String)> = conn.query_row(
        "SELECT at,data_json FROM events WHERE agent_id=? AND kind='execution_failure_v1' ORDER BY seq LIMIT 1",
        [id], |r| Ok((r.get(0)?,r.get(1)?))
    ).optional()?;
    if !matches!(
        status.as_str(),
        "failed" | "timed_out" | "cancelled" | "lost"
    ) && failure.is_none()
    {
        return Ok(());
    }
    if matches!(
        status.as_str(),
        "failed" | "timed_out" | "cancelled" | "lost"
    ) {
        insert(
            conn,
            id,
            "terminal",
            finished
                .filter(|v| v.is_finite() && *v >= 0.0)
                .unwrap_or(at),
            json!({"version":1,"status":status,"exit_code":exit,"finished_at":finished}),
        )?;
    }
    if let Some((when, raw)) = failure {
        let value: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
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
                    "integrity_failure"
                        | "persistence_sqlite"
                        | "io_failure"
                        | "checkpoint_failure"
                )
            })
            .unwrap_or("unknown");
        let mut safe =
            json!({"version":1,"class":class,"stage":stage,"ownership_category":category});
        for key in ["sqlite_extended_code", "os_code"] {
            safe[key] = value[key]
                .as_i64()
                .filter(|n| i32::try_from(*n).is_ok())
                .map(Value::from)
                .unwrap_or(Value::Null);
        }
        insert(conn, id, "execution_failure", when, safe)?;
    }
    let cleanup: Option<(f64,String)>=conn.query_row(
        "SELECT at,data_json FROM events WHERE agent_id=? AND kind='process_cleanup' ORDER BY seq DESC LIMIT 1",
        [id], |r|Ok((r.get(0)?,r.get(1)?))
    ).optional()?;
    if let Some((when, raw)) = cleanup {
        let v: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
        insert(
            conn,
            id,
            "cleanup",
            when,
            json!({"version":1,"confirmed":v["confirmed"].as_bool(),"group_gone":v["group_gone"].as_bool(),"descendants_gone":v["descendants_gone"].as_bool()}),
        )?;
    }
    let mut stmt=conn.prepare("SELECT state,attempts,ambiguous_result FROM deliveries WHERE agent_id=? AND terminal_event_seq IS NOT NULL ORDER BY terminal_event_seq LIMIT 2")?;
    let rows = stmt
        .query_map([id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, bool>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);
    for (state, attempts, ambiguous) in rows {
        if matches!(
            state.as_str(),
            "waiting_binding"
                | "pending"
                | "sending"
                | "delivered"
                | "retry_wait"
                | "failed"
                | "cancelled"
                | "expired"
        ) {
            insert(
                conn,
                id,
                &format!("delivery_{state}"),
                at,
                json!({"version":1,"state":state,"attempts":attempts,"ambiguous":ambiguous}),
            )?;
        }
    }
    Ok(())
}

/// Creates one immutable execution/phase projection. JSON contains only fields
/// selected above; invalid clocks and oversized encoding refuse before mutation.
fn insert(conn: &Connection, id: &str, phase: &str, at: f64, value: Value) -> Result<()> {
    if !at.is_finite() || at < 0.0 {
        return Err(invalid("incident phase time is invalid"));
    }
    let bytes = serde_json::to_string(&value)?;
    if bytes.len() > MAX_BYTES {
        return Err(invalid("incident projection exceeds four KiB"));
    }
    conn.execute("INSERT INTO incident_ledger(execution_id,phase,occurred_at,details_json) VALUES(?,?,?,?) ON CONFLICT(execution_id,phase) DO NOTHING",params![id,phase,at,bytes])?;
    // The independent ledger has a hard record cap even when ordinary
    // retirement is idle; no old agent/transcript is deleted to enforce it.
    conn.execute("DELETE FROM incident_ledger WHERE rowid IN (SELECT rowid FROM incident_ledger ORDER BY occurred_at DESC,execution_id DESC,phase DESC LIMIT -1 OFFSET ?1)", [MAX_RECORDS])?;
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
pub fn prune(conn: &Connection, at: f64) -> Result<usize> {
    if !at.is_finite() || at < 0.0 {
        return Err(invalid("incident retention time is invalid"));
    }
    Ok(conn.execute("DELETE FROM incident_ledger WHERE rowid IN (SELECT rowid FROM incident_ledger WHERE occurred_at < ?1 ORDER BY occurred_at,execution_id,phase LIMIT 2000)",[at-HISTORY_SECONDS])?
       +conn.execute("DELETE FROM incident_ledger WHERE rowid IN (SELECT rowid FROM incident_ledger ORDER BY occurred_at DESC,execution_id DESC,phase DESC LIMIT 2000 OFFSET ?1)",[MAX_RECORDS])?)
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
