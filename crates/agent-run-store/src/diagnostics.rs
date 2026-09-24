//! Bounded read-only diagnostics and model-visible active context snapshots.

use crate::{accounts::account_record, Store, ACTIVE_SQL, VERSION};
use agent_run_domain::{
    catalog::{AccountId, AccountRecord},
    error::invalid,
    Result,
};
use rusqlite::{types::ValueRef, Connection, OpenFlags, OptionalExtension, Row};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeSet, path::Path};

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

/// Reads only configured account identities from the current WAL-aware store
/// without migration or SQL writes, so doctor sees newly registered accounts.
pub fn provider_accounts_snapshot(
    path: &Path,
    ids: &BTreeSet<AccountId>,
) -> Result<Vec<AccountRecord>> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.pragma_update(None, "query_only", true)?;
    let version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version != VERSION {
        return Err(invalid(
            "state migration required before provider diagnosis",
        ));
    }
    let mut statement = conn.prepare(
        "SELECT account_id,auth_family,secret_ref,status FROM provider_accounts WHERE account_id=?",
    )?;
    let mut accounts = Vec::with_capacity(ids.len());
    for id in ids {
        let row = statement
            .query_row([id.as_str()], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .optional()?;
        if let Some(row) = row {
            accounts.push(account_record(row)?);
        }
    }
    Ok(accounts)
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
                    // Named, not positional: later migrations append agents columns.
                    let warned: bool =
                        row.get::<_, bool>("warned")? || row.get::<_, bool>("activity_warned")?;
                    let silence_seconds: f64 = row.get("activity_silence")?;
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
