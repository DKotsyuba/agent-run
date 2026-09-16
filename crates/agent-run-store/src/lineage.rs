//! Safe durable resume lineage admission.
//!
//! Parent validation and the child insert run inside the same immediate SQLite
//! transaction; the partial unique index remains the final authority on forks.

use crate::Record;
use agent_run_domain::{domain::AgentId, error::invalid, Result};
use rusqlite::Transaction;

/// Verified parent facts a child must inherit when continuing one native runtime session.
pub(crate) struct ResumeLineage {
    /// The root agent of the immutable chain.
    pub root_agent_id: AgentId,
    /// The child's one-based position in that chain.
    pub sequence: u32,
    /// The last verified runtime session identity to attach to.
    pub runtime_session_id: String,
}

/// Reports whether a recorded process group is provably absent without signalling it.
fn group_is_gone(pgid: i32) -> bool {
    if pgid <= 1 {
        return false;
    }
    // SAFETY: signal 0 probes exactly this positive process group and has no side effect.
    (unsafe { libc::killpg(pgid, 0) }) != 0
        && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

/// Loads and proves a terminal parent can safely supply exactly one resume child.
pub(crate) fn resume_parent(tx: &Transaction<'_>, parent_id: &AgentId) -> Result<ResumeLineage> {
    let parent = tx.query_row(
        "SELECT * FROM agents WHERE id=?",
        [parent_id.as_str()],
        Record::read,
    )?;
    if !parent.status.terminal() {
        return Err(invalid(format!(
            "agent {parent_id} is not resumable in status {}",
            parent.status.as_str()
        )));
    }
    let quiescent = match parent.process_group_id {
        Some(pgid) => group_is_gone(pgid),
        None => parent.supervisor_pid.is_none(),
    };
    if !quiescent {
        return Err(invalid(format!("agent {parent_id} finished but its runtime process is still alive or unprovable; refusing to attach to a session it may own")));
    }
    let Some(runtime_session_id) = parent
        .runtime_session_id
        .or(parent.resume_of_runtime_session_id)
    else {
        return Err(invalid(format!(
            "agent {parent_id} recorded no runtime session to resume"
        )));
    };
    if runtime_session_id.trim().is_empty() {
        return Err(invalid(format!(
            "agent {parent_id} recorded no runtime session to resume"
        )));
    }
    let sequence = parent
        .sequence
        .checked_add(1)
        .ok_or_else(|| invalid("lineage sequence overflow"))?;
    Ok(ResumeLineage {
        root_agent_id: parent.root_agent_id,
        sequence,
        runtime_session_id,
    })
}
