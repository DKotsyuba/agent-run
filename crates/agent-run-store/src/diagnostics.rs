//! Bounded read-only diagnostics and model-visible active context snapshots.

use crate::{Store, ACTIVE_SQL, VERSION};
use agent_run_domain::{error::invalid, Result};
use rusqlite::{types::ValueRef, Connection, OpenFlags, Row};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::Path;

/// A doctor-safe point-in-time view of active agents and newest capacity samples per identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticSnapshot {
    /// Active agent rows ordered by newest admission, with no parsed secret-bearing fields.
    pub agents: Vec<Value>,
    /// Newest sample for each `(runtime, lane, window, target, source)` identity.
    pub capacity: Vec<Value>,
}

/// Converts a SQLite row into the unmodified Python `dict(row)` wire representation.
fn row_object(row: &Row<'_>) -> rusqlite::Result<Value> {
    let mut object = serde_json::Map::new();
    for index in 0..row.as_ref().column_count() {
        let value = match row.get_ref(index)? {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(value) => Value::from(value),
            ValueRef::Real(value) => serde_json::Number::from_f64(value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            ValueRef::Text(value) => Value::String(String::from_utf8_lossy(value).into_owned()),
            ValueRef::Blob(value) => Value::Array(value.iter().copied().map(Value::from).collect()),
        };
        object.insert(row.as_ref().column_name(index)?.to_owned(), value);
    }
    Ok(Value::Object(object))
}

/// Opens an existing current database read-only and returns Python-compatible bounded diagnostics.
pub fn diagnostic_snapshot(
    path: &Path,
    observed_at: f64,
    limit: usize,
) -> Result<DiagnosticSnapshot> {
    if !observed_at.is_finite() || observed_at < 0.0 {
        return Err(invalid("timestamp must be finite and nonnegative"));
    }
    if limit == 0 || limit > 1000 {
        return Err(invalid("limit must be 1..1000"));
    }
    if !path.is_file() {
        return Err(invalid(format!(
            "state database does not exist: {}",
            path.display()
        )));
    }
    let uri = format!("file:{}?mode=ro&immutable=1", path.display());
    let conn = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    conn.pragma_update(None, "query_only", true)?;
    let version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version != VERSION {
        return Err(invalid(format!(
            "state migration required: found v{version}, expected v{VERSION}"
        )));
    }
    let mut agents_stmt = conn.prepare(&format!("SELECT * FROM agents WHERE status IN {ACTIVE_SQL} ORDER BY created_at DESC,id DESC LIMIT ?"))?;
    let agents = agents_stmt
        .query_map([limit as i64], row_object)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut capacity_stmt = conn.prepare("SELECT id,runtime,lane,window,target,source,observed_at,valid_until FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY runtime,lane,window,target,source ORDER BY observed_at DESC,id DESC) AS position FROM capacity_samples) WHERE position=1 ORDER BY observed_at DESC,id DESC LIMIT ?")?;
    let capacity = capacity_stmt.query_map([limit as i64], |row| Ok(json!({ "id": row.get::<_, i64>(0)?, "runtime": row.get::<_, String>(1)?, "lane": row.get::<_, String>(2)?, "window": row.get::<_, String>(3)?, "target": row.get::<_, Option<String>>(4)?, "source": row.get::<_, String>(5)?, "observed_at": row.get::<_, Option<f64>>(6)?, "valid_until": row.get::<_, Option<f64>>(7)? })))?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(DiagnosticSnapshot { agents, capacity })
}

impl Store {
    /// Returns active agents visible to one orchestrator session, newest first, with silence and warning state.
    pub fn active_context(
        &self,
        orchestrator_session_id: &str,
        observed_at: f64,
        limit: usize,
    ) -> Result<Vec<Value>> {
        if orchestrator_session_id.trim().is_empty() {
            return Err(invalid("orchestrator_session_id must be a nonblank string"));
        }
        if !observed_at.is_finite() || observed_at < 0.0 {
            return Err(invalid("timestamp must be finite and nonnegative"));
        }
        if limit == 0 || limit > 1000 {
            return Err(invalid("limit must be 1..1000"));
        }
        let mut stmt = self.conn.prepare(&format!("SELECT a.*,EXISTS(SELECT 1 FROM events e WHERE e.agent_id=a.id AND e.kind='deadline_warning') AS activity_warned,MAX(0.0,?-COALESCE((SELECT MAX(m.at) FROM messages m WHERE m.agent_id=a.id),a.started_at,a.created_at)) AS activity_silence FROM agents a WHERE a.orchestrator_session_id=? AND a.status IN {ACTIVE_SQL} ORDER BY a.created_at DESC,a.id DESC LIMIT ?"))?;
        let rows = stmt
            .query_map(
                (observed_at, orchestrator_session_id, limit as i64),
                |row| {
                    let warned: bool = row.get::<_, bool>(24)? || row.get::<_, bool>(38)?;
                    let silence_seconds: f64 = row.get(39)?;
                    let mut object = row_object(row)?.as_object().cloned().expect("row object");
                    object.remove("activity_warned");
                    object.remove("activity_silence");
                    object.insert("warned".to_owned(), Value::Bool(warned));
                    object.insert("silent_seconds".to_owned(), json!(silence_seconds));
                    Ok(Value::Object(object))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
}
