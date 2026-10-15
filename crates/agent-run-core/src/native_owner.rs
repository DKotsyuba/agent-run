//! Native owner admission fences shared by the existing Codex and Claude event loops.
use crate::{Result, persist::Persist, profiles::Profile, state::Record};
use agent_run_adapters::{
    io::Process,
    native_owner::{self, Call},
};
use agent_run_domain::domain::{Constraint, Status};
use agent_run_platform::process;
use std::collections::BTreeMap;

/// Strip retired build qualifiers from every launch and remove owner settings for one-shot, restricted or unsupported launches.
pub fn configure(
    environment: &mut BTreeMap<String, String>,
    record: &Record,
    role: &Profile,
    supported: bool,
) {
    for key in native_owner::RETIRED_SETTINGS {
        environment.remove(key);
    }
    if !supported
        || !record.request.explicit_finish
        || role.research_tools_only()
        || role
            .required_constraints
            .contains(&Constraint::UnixIpcIsolation)
        || role
            .required_constraints
            .contains(&Constraint::McpIpcIsolation)
    {
        for key in native_owner::SETTINGS {
            environment.remove(key);
        }
        environment.remove(native_owner::LOCATOR);
    }
}
/// Refuse before effect unless the same owned child/session and original logical admission are still live and positively idle.
/// Storage/identity failures are unavailable evidence, never permission to wake an unknown or terminal actor.
pub fn refusal(
    process: &Process,
    store: &Persist,
    record: &Record,
    call: &Call,
    session: Option<&str>,
    idle: bool,
    closing: bool,
) -> Result<Option<&'static str>> {
    if tokio::time::Instant::now() >= call.deadline {
        return Ok(Some("unavailable"));
    }
    let Some(native) = process.owner.leader.as_ref() else {
        return Ok(Some("unavailable"));
    };
    if native.pid as u32 != call.request.native_pid
        || native.birth.floor() as u64 != call.request.native_start_time
        || session != Some(call.request.thread_id.as_str())
    {
        return Ok(Some("actor_mismatch"));
    }
    if process::observe(Some(native.pid), Some(&native.token), Some(native.birth))
        != process::ProcessState::Alive
        || process.input.is_none()
    {
        return Ok(Some("unavailable"));
    }
    let actual = process::inspect(native.pid)?;
    if actual.ppid != std::process::id() as i32
        || actual.group != native.group
        || actual.token != native.token
        || actual.birth != native.birth
    {
        return Ok(Some("actor_mismatch"));
    }
    let current = store.get(&record.id)?;
    if current.status != Status::Running
        || closing
        || store.worker_finish_intent(&record.id, false)?.is_some()
    {
        return Ok(Some("finished"));
    }
    if serde_json::to_value(&current.request)? != serde_json::to_value(&record.request)?
        || current.sequence != record.sequence
        || current.identity != record.identity
    {
        return Ok(Some("actor_mismatch"));
    }
    let owned:bool=store.conn.query_row("SELECT EXISTS(SELECT 1 FROM attempts WHERE agent_id=? AND ownership_active=1 AND finished_at IS NULL AND process_identity=? AND process_birth_time=?)",rusqlite::params![record.id.as_str(),native.token,native.birth],|row|row.get(0))?;
    if !owned {
        return Ok(Some("actor_mismatch"));
    }
    if store.cancel_pending(&record.id)? {
        return Ok(Some("cancelled"));
    }
    if !idle {
        return Ok(Some("not_idle"));
    }
    let Some(lifecycle) = store.worker_lifecycle_view(&record.id)? else {
        return Ok(Some("unavailable"));
    };
    if lifecycle["phase"] != "idle" {
        return Ok(Some("not_idle"));
    }
    Ok(None)
}
