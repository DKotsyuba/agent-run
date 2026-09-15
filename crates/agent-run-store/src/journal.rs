//! Durable event, command, attempt, and transcript journal operations.
//!
//! Each mutating operation opens and commits its own immediate SQLite transaction;
//! callers must keep a [`Store`] on the thread that created its connection.

use crate::Store;
use agent_run_domain::{
    domain::{now, AgentId},
    error::invalid,
    Error, Result,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde_json::Value;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Component, Path},
};

/// Python's maximum UTF-8 byte length retained directly in a `messages` row.
pub const MAX_INLINE_MESSAGE_BYTES: usize = 32 * 1024;
/// Number of Unicode scalar values retained at the front of an oversized stub.
pub const INLINE_STUB_HEAD_CHARS: usize = 4096;

/// Rejects a raw transcript reference that could escape the owning agent directory.
fn valid_raw_ref(raw_ref: &str) -> bool {
    !raw_ref.is_empty()
        && !raw_ref.contains('\\')
        && Path::new(raw_ref)
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
}

/// Writes an oversized transcript body beneath its agent directory and returns its stub/reference.
fn spool(home: &Path, id: &AgentId, content: &str) -> Result<(String, String)> {
    let directory = home.join("agents").join(id.as_str());
    fs::create_dir_all(&directory)?;
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
    let bytes = content.as_bytes();
    for _ in 0..16 {
        let name = format!("message.{}.raw", uuid::Uuid::new_v4().simple());
        let path = directory.join(&name);
        let opened = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path);
        let Ok(mut file) = opened else { continue };
        let write = (|| -> std::io::Result<()> {
            file.write_all(bytes)?;
            file.sync_all()
        })();
        if let Err(error) = write {
            let _ = fs::remove_file(path);
            return Err(error.into());
        }
        let stub = format!(
            "{}\n[...spooled: {} bytes exceed the 32 KiB inline limit; full content in raw_ref={name}]",
            content.chars().take(INLINE_STUB_HEAD_CHARS).collect::<String>(),
            bytes.len(),
        );
        return Ok((stub, name));
    }
    Err(Error::Runtime(
        "unable to allocate transcript spool file".into(),
    ))
}

/// Stores a journal message's inline body or atomically creates its bounded raw spool companion.
pub(crate) fn message_storage(
    home: &Path,
    id: &AgentId,
    content: &str,
    raw_ref: Option<&str>,
) -> Result<(String, Option<String>)> {
    if raw_ref.is_some_and(|value| !valid_raw_ref(value)) {
        return Err(invalid("raw_ref must be a normalized relative path"));
    }
    if content.len() <= MAX_INLINE_MESSAGE_BYTES {
        return Ok((content.to_owned(), raw_ref.map(str::to_owned)));
    }
    let (stub, reference) = spool(home, id, content)?;
    Ok((stub, Some(reference)))
}

impl Store {
    /// Creates the next numbered adapter attempt for an existing agent and returns its durable id.
    pub fn create_attempt(
        &mut self,
        id: &AgentId,
        state: &str,
        adapter_state: &Value,
    ) -> Result<String> {
        if state.trim().is_empty() {
            return Err(invalid("attempt state must be a nonblank string"));
        }
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
        let number: i64 = tx.query_row(
            "SELECT COALESCE(MAX(number),0)+1 FROM attempts WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )?;
        let attempt_id = format!("att_{}", uuid::Uuid::new_v4().simple());
        tx.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at) VALUES(?,?,?,?,?,?)", params![attempt_id, id.as_str(), number, state, serde_json::to_string(adapter_state)?, now()])?;
        tx.commit()?;
        Ok(attempt_id)
    }

    /// Finishes one still-open attempt owned by `id`; foreign, missing, and finished attempts conflict.
    pub fn finish_attempt(&mut self, id: &AgentId, attempt_id: &str, state: &str) -> Result<()> {
        if state.trim().is_empty() {
            return Err(invalid("attempt state must be a nonblank string"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute("UPDATE attempts SET state=?,finished_at=? WHERE id=? AND agent_id=? AND finished_at IS NULL", params![state, now(), attempt_id, id.as_str()])?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.commit()?;
        Ok(())
    }

    /// Appends an event after checking the agent and optional attempt ownership, returning its global revision.
    pub fn append_event(
        &mut self,
        id: &AgentId,
        kind: &str,
        data: &Value,
        attempt_id: Option<&str>,
    ) -> Result<i64> {
        if kind.trim().is_empty() {
            return Err(invalid("event kind must be a nonblank string"));
        }
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
        if let Some(attempt_id) = attempt_id {
            let owned: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM attempts WHERE id=? AND agent_id=?)",
                params![attempt_id, id.as_str()],
                |row| row.get(0),
            )?;
            if !owned {
                return Err(invalid("attempt is unknown or owned by another agent"));
            }
        }
        tx.execute(
            "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,?,?)",
            params![
                id.as_str(),
                attempt_id,
                now(),
                kind,
                serde_json::to_string(data)?
            ],
        )?;
        let seq = tx.last_insert_rowid();
        tx.commit()?;
        Ok(seq)
    }

    /// Appends one timestamped transcript row, spooling bodies above 32 KiB and checking attempt ownership.
    pub fn append_message(
        &mut self,
        id: &AgentId,
        role: &str,
        content: &str,
        name: Option<&str>,
        raw_ref: Option<&str>,
        attempt_id: Option<&str>,
    ) -> Result<i64> {
        if !["user", "assistant", "system", "tool_call", "tool_result"].contains(&role) {
            return Err(invalid("unknown transcript role"));
        }
        let (content, raw_ref) = message_storage(&self.home, id, content, raw_ref)?;
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
        if let Some(attempt_id) = attempt_id {
            let owned: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM attempts WHERE id=? AND agent_id=?)",
                params![attempt_id, id.as_str()],
                |row| row.get(0),
            )?;
            if !owned {
                return Err(invalid("attempt is unknown or owned by another agent"));
            }
        }
        tx.execute("INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref) VALUES(?,?,?,?,?,?,?)", params![id.as_str(), attempt_id, now(), role, name, content, raw_ref])?;
        let seq = tx.last_insert_rowid();
        tx.commit()?;
        Ok(seq)
    }

    /// Claims the oldest pending command, preferring cancellation over steering, exactly once.
    pub fn claim_command(&mut self, id: &AgentId) -> Result<Option<(i64, String, Value)>> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = tx.query_row("SELECT id,kind,payload_json FROM commands WHERE agent_id=? AND state='pending' ORDER BY CASE kind WHEN 'cancel' THEN 0 ELSE 1 END,id LIMIT 1", [id.as_str()], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))).optional()?;
        let Some((command_id, kind, payload)) = row else {
            tx.commit()?;
            return Ok(None);
        };
        let changed = tx.execute(
            "UPDATE commands SET state='claimed',claimed_at=? WHERE id=? AND state='pending'",
            params![now(), command_id],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.commit()?;
        Ok(Some((command_id, kind, serde_json::from_str(&payload)?)))
    }

    /// Completes a claimed command only when it is owned by the supplied agent.
    pub fn complete_command(
        &mut self,
        id: &AgentId,
        command_id: i64,
        result: &Value,
    ) -> Result<()> {
        let changed = self.conn.execute("UPDATE commands SET state='completed',completed_at=?,result_json=? WHERE id=? AND agent_id=? AND state='claimed'", params![now(), serde_json::to_string(result)?, command_id, id.as_str()])?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}
