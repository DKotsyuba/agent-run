//! Durable event, command, attempt, and transcript journal operations.
//!
//! Claims/appends use short immediate transactions; an empty command poll is
//! read-only and acknowledgements use one autocommit statement. SQL failures
//! carry closed operation labels and elapsed time, without generic replay;
//! callers must keep a [`Store`] on the thread that created its connection.

use crate::Store;
use agent_run_domain::{
    Error, Result,
    domain::{AgentId, now},
    error::invalid,
};
use rusqlite::{OptionalExtension, params};
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
            content
                .chars()
                .take(INLINE_STUB_HEAD_CHARS)
                .collect::<String>(),
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

impl Store<'_> {
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
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
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
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
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
        crate::persistence(
            agent_run_domain::error::PersistenceOperation::EventAppend,
            || {
                if kind.trim().is_empty() {
                    return Err(invalid("event kind must be a nonblank string"));
                }
                let tx = crate::writer::WriteTx::begin(&self.conn)?;
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
            },
        )
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
        crate::persistence(
            agent_run_domain::error::PersistenceOperation::TranscriptAppend,
            || {
                if !["user", "assistant", "system", "tool_call", "tool_result"].contains(&role) {
                    return Err(invalid("unknown transcript role"));
                }
                let content_complete = content.len() <= MAX_INLINE_MESSAGE_BYTES;
                let (content, raw_ref) = message_storage(&self.home, id, content, raw_ref)?;
                let tx = crate::writer::WriteTx::begin(&self.conn)?;
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
                tx.execute("INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref,content_complete,root_agent_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,(SELECT COALESCE(NULLIF(root_agent_id,''),id) FROM agents WHERE id=?1))", params![id.as_str(), attempt_id, now(), role, name, content, raw_ref, content_complete])?;
                let seq = tx.last_insert_rowid();
                tx.commit()?;
                Ok(seq)
            },
        )
    }

    /// Claims the oldest pending command, preferring cancellation over
    /// steering over pool delivery, exactly once.
    pub fn claim_command(&mut self, id: &AgentId) -> Result<Option<(i64, String, Value)>> {
        self.claim_command_for_turn(id, true)
    }

    /// Claim control commands normally, but hold native-completion wakes until
    /// an explicit turn is idle. Cancellation always remains claimable. This
    /// preserves the existing queue/priority and avoids overlapping model turns.
    /// A read-only eligibility probe skips empty/held-wake write transactions;
    /// acquisition rechecks under the writer lock, so concurrent claims remain
    /// exactly once. None can delay a newly enqueued command until the next poll.
    /// SQL errors identify CommandClaim/timing/count=1; claimed work never replays.
    pub fn claim_command_for_turn(
        &mut self,
        id: &AgentId,
        allow_native_wake: bool,
    ) -> Result<Option<(i64, String, Value)>> {
        crate::persistence(
            agent_run_domain::error::PersistenceOperation::CommandClaim,
            || {
                // Idle control reads never contend for the single WAL writer. Eligibility
                // is rechecked inside the immediate claim transaction when work exists.
                let pending: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM commands WHERE agent_id=? AND state='pending' AND (? OR kind='cancel' OR COALESCE(json_extract(payload_json,'$.native_completion'),0)<>1))",
            params![id.as_str(), allow_native_wake], |row| row.get(0),
        )?;
                if !pending {
                    return Ok(None);
                }
                let tx = crate::writer::WriteTx::begin(&self.conn)?;
                let row = tx.query_row("SELECT id,kind,payload_json FROM commands WHERE agent_id=? AND state='pending' \
            AND (? OR kind='cancel' OR COALESCE(json_extract(payload_json,'$.native_completion'),0)<>1) \
            ORDER BY CASE kind WHEN 'cancel' THEN 0 WHEN 'steer' THEN 1 ELSE 2 END,id LIMIT 1", params![id.as_str(),allow_native_wake], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))).optional()?;
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
            },
        )
    }

    /// Completes a claimed command only when it is owned by the supplied agent.
    /// SQL errors identify CommandWrite/timing/count=1. An uncertain ack never
    /// replays the model, tool or control action; non-claimed state is a conflict.
    pub fn complete_command(
        &mut self,
        id: &AgentId,
        command_id: i64,
        result: &Value,
    ) -> Result<()> {
        crate::persistence(
            agent_run_domain::error::PersistenceOperation::CommandWrite,
            || {
                let changed = self.conn.execute("UPDATE commands SET state='completed',completed_at=?,result_json=? WHERE id=? AND agent_id=? AND state='claimed'", params![now(), serde_json::to_string(result)?, command_id, id.as_str()])?;
                if changed != 1 {
                    return Err(Error::Conflict);
                }
                Ok(())
            },
        )
    }
}

/// Builds one transcript journal fragment on the producer side: bodies above
/// 32 KiB are spooled to a private file under the agent directory (filesystem
/// work that must never run on the writer thread) and replaced by a stub with
/// `content_complete=false`. Validation of role and error provenance happens
/// again when the fragment is applied.
#[allow(clippy::too_many_arguments)]
pub fn prepare_message_fragment(
    home: &Path,
    id: &AgentId,
    role: &str,
    text: &str,
    name: Option<&str>,
    raw_ref: Option<&str>,
    attempt: Option<&str>,
    error: Option<(bool, &str)>,
) -> Result<crate::ops::JournalFragment> {
    let content_complete = text.len() <= MAX_INLINE_MESSAGE_BYTES;
    let (content, raw_ref) = message_storage(home, id, text, raw_ref)?;
    Ok(crate::ops::JournalFragment::Message {
        role: role.to_owned(),
        content,
        content_complete,
        name: name.map(str::to_owned),
        raw_ref,
        attempt: attempt.map(str::to_owned),
        error: error.map(|(failed, source)| (failed, source.to_owned())),
    })
}

/// Native tool error provenance labels accepted on `tool_result` rows.
const ERROR_SOURCES: [&str; 6] = [
    "opencode.tool.state",
    "claude.is_error",
    "codex.command.exitCode",
    "codex.command.status",
    "codex.mcp.status",
    "codex.mcp.error",
];

impl Store<'_> {
    /// Appends one prepared journal fragment inside the caller's write scope
    /// and returns its row sequence. Events check the agent and optional
    /// attempt ownership exactly like [`Store::append_event`]; messages require
    /// already-spooled inline content (<= 32 KiB), a known role, valid error
    /// provenance and a normalized raw reference, and never touch the
    /// filesystem. Unknown agents are `NotFound`.
    pub fn append_fragment(
        &mut self,
        id: &AgentId,
        fragment: &crate::ops::JournalFragment,
    ) -> Result<i64> {
        match fragment {
            crate::ops::JournalFragment::Event {
                kind,
                data,
                attempt,
            } => self.append_event(id, kind, data, attempt.as_deref()),
            crate::ops::JournalFragment::Message {
                role,
                content,
                content_complete,
                name,
                raw_ref,
                attempt,
                error,
            } => {
                if !["user", "assistant", "system", "tool_call", "tool_result"]
                    .contains(&role.as_str())
                {
                    return Err(invalid("unknown transcript role"));
                }
                if content.len() > MAX_INLINE_MESSAGE_BYTES {
                    return Err(invalid(
                        "journal fragment content must be spooled by the producer",
                    ));
                }
                if error.as_ref().is_some_and(|(_, source)| {
                    role != "tool_result" || !ERROR_SOURCES.contains(&source.as_str())
                }) {
                    return Err(invalid("invalid native tool error evidence"));
                }
                if raw_ref
                    .as_deref()
                    .is_some_and(|value| !valid_raw_ref(value))
                {
                    return Err(invalid("raw_ref must be a normalized relative path"));
                }
                let tx = crate::writer::WriteTx::begin(&self.conn)?;
                let exists: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM agents WHERE id=?)",
                    [id.as_str()],
                    |row| row.get(0),
                )?;
                if !exists {
                    return Err(Error::NotFound(id.to_string()));
                }
                if let Some(attempt) = attempt {
                    let owned: bool = tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM attempts WHERE id=? AND agent_id=?)",
                        params![attempt, id.as_str()],
                        |row| row.get(0),
                    )?;
                    if !owned {
                        return Err(invalid("attempt is unknown or owned by another agent"));
                    }
                }
                tx.execute(
                    "INSERT INTO messages(agent_id,attempt_id,at,role,name,content,raw_ref,error,error_source,content_complete,root_agent_id) \
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,(SELECT COALESCE(NULLIF(root_agent_id,''),id) FROM agents WHERE id=?1))",
                    params![
                        id.as_str(),
                        attempt,
                        now(),
                        role,
                        name,
                        content,
                        raw_ref,
                        error.as_ref().map(|(failed, _)| *failed),
                        error.as_ref().map(|(_, source)| source.as_str()),
                        content_complete
                    ],
                )?;
                let seq = tx.last_insert_rowid();
                tx.commit()?;
                Ok(seq)
            }
        }
    }
}

impl Store<'_> {
    /// Completes every still-pending command of a terminal execution with
    /// its stable terminal result (`cancel` accepted as already stopping,
    /// others rejected as `agent_terminal`), recording `at` as claim and
    /// completion time. Claimed commands are untouched: replaying them could
    /// duplicate an engine action. A nonterminal execution is a transition
    /// error; an unknown one is `NotFound`. Returns the number completed.
    pub fn complete_terminal_commands(&mut self, id: &AgentId, at: f64) -> Result<u64> {
        if !at.is_finite() || at < 0.0 {
            return Err(invalid("completion time must be finite and nonnegative"));
        }
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let status: Option<String> = tx
            .query_row(
                "SELECT status FROM agents WHERE id=?",
                [id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(status) = status else {
            return Err(Error::NotFound(id.to_string()));
        };
        if !matches!(
            status.as_str(),
            "succeeded" | "failed" | "cancelled" | "lost" | "timed_out"
        ) {
            return Err(Error::Transition(
                "terminal command completion requires a terminal agent".into(),
            ));
        }
        let completed = tx.execute(
            "UPDATE commands SET state='completed',claimed_at=?1,completed_at=?1,result_json=CASE kind \
             WHEN 'cancel' THEN '{\"accepted\":true,\"reason\":\"already_stopping\"}' \
             ELSE '{\"accepted\":false,\"reason\":\"agent_terminal\"}' END \
             WHERE agent_id=?2 AND state='pending'",
            params![at, id.as_str()],
        )?;
        tx.commit()?;
        Ok(completed as u64)
    }
}
