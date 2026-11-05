//! Safe durable resume lineage admission.
//!
//! Parent validation and the child insert run inside the same immediate SQLite
//! transaction; the partial unique index remains the final authority on forks.

use crate::Record;
use agent_run_domain::{
    Result,
    domain::{AgentId, Status},
    error::invalid,
};
use rusqlite::OptionalExtension;

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

/// A maintenance supervisor that died before native launch is quiescent only
/// after its exact identity is gone and every attempt has cleanup proof.
fn prepared_maintenance_is_quiescent(tx: &rusqlite::Connection, parent: &Record) -> Result<bool> {
    let target_state = if parent.status == Status::Lost
        && parent.failure_kind.as_deref() == Some(crate::reconcile::MAINTENANCE_RESUME_START_FAILED)
    {
        "lost"
    } else if parent.status == Status::Cancelled {
        "cancelled"
    } else {
        return Ok(false);
    };
    let owner = match (
        parent.supervisor_pid,
        parent.supervisor_identity.as_deref(),
        parent.supervisor_birth_time,
    ) {
        (Some(pid), Some(token), Some(birth)) => Some((pid, token.to_owned(), birth, None)),
        (None, None, None) => {
            let (startup_owner, startup_birth_time): (Option<String>, Option<f64>) = tx.query_row(
                "SELECT startup_owner_pid_identity,startup_owner_birth_time FROM agents WHERE id=?",
                [parent.id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let expected = crate::ops::ExpectedOwnership {
                execution: parent.id.clone(),
                supervisor_pid: None,
                supervisor_identity: None,
                supervisor_birth_time: None,
                process_group_id: None,
                startup_owner,
                startup_birth_time,
            };
            crate::maintenance_target_cancel::startup_child(&expected)
                .map(|child| (child.pid, child.token, child.birth, None))
                .or_else(|| {
                    crate::maintenance_target_cancel::startup_broker(&expected).map(
                        |(pid, token, birth)| (pid, token, birth, expected.startup_owner.clone()),
                    )
                })
        }
        _ => None,
    };
    let Some((pid, token, birth, broker_owner)) = owner else {
        return Ok(false);
    };
    let state = agent_run_platform::process::observe(Some(pid), Some(&token), Some(birth));
    let target: Option<String> = tx.query_row(
        "SELECT t.id FROM maintenance_resumes r JOIN attempts t ON t.id=r.target_attempt_id AND t.agent_id=r.agent_id \
         WHERE r.agent_id=?1 AND t.state=?2 AND t.phase='cleanup_complete' AND json_extract(t.cleanup_proof_json,'$.never_spawned')=1 \
         ORDER BY r.epoch DESC LIMIT 1",
        rusqlite::params![parent.id.as_str(),target_state], |row| row.get(0),
    ).optional()?;
    let Some(target) = target else {
        return Ok(false);
    };
    if !crate::maintenance_target_cancel::preparation_is_settled(tx, &parent.id, &target)? {
        return Ok(false);
    }
    if state == agent_run_platform::process::ProcessState::Alive {
        let Some(raw_owner) = broker_owner else {
            return Ok(false);
        };
        if parent.status != Status::Cancelled
            || !crate::maintenance_target_cancel::preparation_not_started(tx, &parent.id, &target)?
            || !tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM writer_authority w JOIN events e ON e.agent_id=?1 AND e.attempt_id=?2 \
                 JOIN maintenance_resumes r ON r.agent_id=e.agent_id AND r.target_attempt_id=e.attempt_id \
                 WHERE w.id=1 AND w.holder_pid=?3 AND w.holder_token=?4 AND w.holder_birth=?5 \
                 AND e.kind='maintenance_current_broker_cancelled_v1' AND json_extract(e.data_json,'$.epoch')=r.epoch \
                 AND json_extract(e.data_json,'$.target_attempt_id')=r.target_attempt_id AND json_extract(e.data_json,'$.owner_sha256')=?6)",
                rusqlite::params![parent.id.as_str(),target,pid,token,birth,agent_run_platform::fs::sha256(raw_owner.as_bytes())],
                |row|row.get::<_,bool>(0),
            )?
        {
            return Ok(false);
        }
    } else if !matches!(
        state,
        agent_run_platform::process::ProcessState::Dead
            | agent_run_platform::process::ProcessState::Reused
    ) {
        return Ok(false);
    }
    Ok(!tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM attempts WHERE agent_id=? AND (cleanup_proof_json IS NULL OR COALESCE(phase,'')!='cleanup_complete'))",
        [parent.id.as_str()], |row| row.get::<_,bool>(0),
    )?)
}

/// Loads and proves a terminal parent can safely supply exactly one resume child.
pub(crate) fn resume_parent(
    tx: &rusqlite::Connection,
    parent_id: &AgentId,
) -> Result<ResumeLineage> {
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
    let prepared_terminal = parent.failure_kind.as_deref() == Some(crate::reconcile::MAINTENANCE_RESUME_START_FAILED)
        || (parent.status == Status::Cancelled && tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM maintenance_resumes r JOIN attempts t ON t.id=r.target_attempt_id AND t.agent_id=r.agent_id \
             WHERE r.agent_id=? AND t.state='cancelled' AND t.process_identity IS NULL)",
            [parent.id.as_str()], |row| row.get::<_,bool>(0),
        )?);
    let quiescent = match parent.process_group_id {
        Some(pgid) => group_is_gone(pgid),
        None if prepared_terminal => prepared_maintenance_is_quiescent(tx, &parent)?,
        None => parent.supervisor_pid.is_none(),
    };
    if !quiescent {
        return Err(invalid(format!(
            "agent {parent_id} finished but its runtime process is still alive or unprovable; refusing to attach to a session it may own"
        )));
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
