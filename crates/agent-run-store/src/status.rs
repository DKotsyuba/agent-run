//! Bounded database-only status proof, over exactly one caller-owned snapshot.

use crate::Store;
use agent_run_domain::{
    ProviderStartRequest, Result,
    batch::ExecutionRef,
    canonical,
    catalog::{AccountId, ResolvedLaunchAuthority},
    domain::{AgentId, Status, now},
    error::invalid,
    status::{
        CleanupFact, CleanupState, Coverage, ExecutionFact, PinState, RestartResume, StatusItem,
        StatusMatch, StatusRequest, StatusResponse, SuccessCommit,
    },
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Reuses retention's bounded 20,000-row and 64 MiB observation budgets.
const MAX_ROWS: usize = 20_000;
const MAX_BYTES: usize = 64 * 1024 * 1024;
/// A single writer admission is already bounded to 16 MiB.
const MAX_IDENTITY_BYTES: usize = 16 * 1024 * 1024;
/// Stored cleanup evidence is small; oversized data cannot prove cleanup.
const MAX_PROOF_BYTES: usize = 65_536;

/// Snapshot result plus non-serialized exact IDs for a future same-snapshot view.
#[derive(Debug)]
pub struct StatusSnapshot<'a> {
    /// Safe compact database facts; no PID, token, or AgentView.
    pub response: StatusResponse,
    /// Internal exact rows corresponding to selected and retained-latest facts.
    selected: Vec<Option<AgentId>>,
    latest: Vec<Option<AgentId>>,
    /// Keeps exact row handles tied to the borrowed snapshot connection.
    snapshot: std::marker::PhantomData<&'a Connection>,
}

impl StatusSnapshot<'_> {
    /// Exact selected row, usable only while the caller keeps this snapshot open.
    pub fn selected_execution(&self, index: usize) -> Option<&AgentId> {
        self.selected.get(index).and_then(Option::as_ref)
    }

    /// Exact retained tail row, not a latest-ever guarantee without coverage.
    pub fn retained_latest_execution(&self, index: usize) -> Option<&AgentId> {
        self.latest.get(index).and_then(Option::as_ref)
    }
}

/// Counts facts actually examined across the whole bounded request.
struct Budget {
    rows: usize,
    bytes: usize,
    deadline: Instant,
    exhausted: bool,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            rows: 0,
            bytes: 0,
            deadline: Instant::now() + Duration::from_secs(2),
            exhausted: false,
        }
    }
}

impl Budget {
    fn row(&mut self) -> bool {
        self.rows += 1;
        let within = self.rows <= MAX_ROWS && Instant::now() < self.deadline;
        self.exhausted |= !within;
        within
    }
    fn bytes(&mut self, count: usize) -> bool {
        self.bytes = self.bytes.saturating_add(count);
        let within = self.bytes <= MAX_BYTES && Instant::now() < self.deadline;
        self.exhausted |= !within;
        within
    }
    fn remaining_rows(&self) -> usize {
        MAX_ROWS.saturating_sub(self.rows)
    }
}

#[derive(Clone)]
struct Execution {
    id: AgentId,
    fact: ExecutionFact,
}

struct Lineage {
    /// Only caller-selected sequences, never the full chain.
    selected: HashMap<u32, Option<Execution>>,
    tail: Option<Execution>,
    structural: bool,
    cleanup: CleanupState,
    restart: RestartResume,
}

impl Lineage {
    fn unknown() -> Self {
        Self {
            selected: HashMap::new(),
            tail: None,
            structural: false,
            cleanup: CleanupState::Unknown,
            restart: RestartResume::Unknown,
        }
    }
}

/// Merges evidence without letting an incomplete proof become a confirmation.
fn merge_cleanup(left: CleanupState, right: CleanupState) -> CleanupState {
    match (left, right) {
        (CleanupState::Unknown, _) | (_, CleanupState::Unknown) => CleanupState::Unknown,
        (CleanupState::Pending, _) | (_, CleanupState::Pending) => CleanupState::Pending,
        _ => CleanupState::Confirmed,
    }
}

fn merge_restart(left: RestartResume, right: RestartResume) -> RestartResume {
    match (left, right) {
        (RestartResume::Pending, _) | (_, RestartResume::Pending) => RestartResume::Pending,
        (RestartResume::Unknown, _) | (_, RestartResume::Unknown) => RestartResume::Unknown,
        _ => RestartResume::None,
    }
}

/// Treats malformed or non-text stored evidence as unavailable, never as proof.
fn optional_text(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<String>> {
    Ok(match row.get_ref(index)? {
        rusqlite::types::ValueRef::Text(bytes) => {
            std::str::from_utf8(bytes).ok().map(str::to_owned)
        }
        _ => None,
    })
}

/// Checks frozen provider-v2 request and authority without resolving paths or reading secrets.
fn provider_identity(conn: &Connection, id: &AgentId, budget: &mut Budget) -> Result<bool> {
    let row: (
        Option<String>,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
    ) = conn.query_row(
        "SELECT CASE WHEN length(CAST(identity_json AS BLOB))<=?2 THEN identity_json END, \
         runtime,model,profile,CASE WHEN length(CAST(task AS BLOB))<=?3 THEN task END, \
         CASE WHEN length(CAST(workdir AS BLOB))<=?2 THEN workdir END FROM agents WHERE id=?1",
        params![
            id.as_str(),
            MAX_IDENTITY_BYTES as i64,
            agent_run_domain::domain::MAX_TASK_BYTES as i64
        ],
        |row| {
            Ok((
                optional_text(row, 0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                optional_text(row, 4)?,
                optional_text(row, 5)?,
            ))
        },
    )?;
    let (Some(raw), runtime, model, profile, Some(task), Some(workdir)) = row else {
        return Ok(false);
    };
    if !budget.bytes(
        raw.len()
            .saturating_add(task.len())
            .saturating_add(workdir.len()),
    ) {
        return Ok(false);
    }
    let Ok(identity) = serde_json::from_str::<Value>(&raw) else {
        return Ok(false);
    };
    let original = &identity["provider_request"];
    if identity["provider_identity_version"] != 2
        || !original.is_object()
        || identity["replay_request_sha256"].as_str()
            != Some(canonical::sha256_hex(original, true).as_str())
    {
        return Ok(false);
    }
    let Ok(mut request) = ProviderStartRequest::from_history(original.clone()) else {
        return Ok(false);
    };
    if request.validate_intent().is_err() {
        return Ok(false);
    }
    let Ok(authority) =
        serde_json::from_value::<ResolvedLaunchAuthority>(identity["authority"].clone())
    else {
        return Ok(false);
    };
    Ok(authority.validate().is_ok()
        && request.provider.as_str() == runtime
        && request.model == model
        && request.profile == profile
        && request.task == task
        && request.workdir.to_string_lossy() == workdir
        && authority.provider == request.provider
        && authority.model == request.model
        && authority.profile == request.profile
        && authority.workdir == request.workdir)
}

/// Validates all retained attempts in sequence, never exposing native identities.
fn attempts(
    conn: &Connection,
    id: &AgentId,
    provider: bool,
    budget: &mut Budget,
) -> Result<(Option<ExecutionRef>, bool, CleanupState)> {
    let mut statement = conn.prepare(
        "SELECT id,number,phase,ownership_active,process_identity IS NOT NULL,process_birth_time IS NOT NULL, \
         CASE WHEN length(CAST(cleanup_proof_json AS BLOB))<=?2 THEN cleanup_proof_json END, \
         selected_account_id,state FROM attempts WHERE agent_id=?1 ORDER BY number LIMIT ?3")?;
    let mut rows = statement.query(params![
        id.as_str(),
        MAX_PROOF_BYTES as i64,
        (budget.remaining_rows() + 1) as i64
    ])?;
    let mut expected = 1_i64;
    let mut first = None;
    let mut complete = provider;
    let mut cleanup = CleanupState::Confirmed;
    while let Some(row) = rows.next()? {
        if !budget.row() {
            return Ok((None, false, CleanupState::Unknown));
        }
        let attempt: String = row.get(0)?;
        let number: i64 = row.get(1)?;
        let phase = optional_text(row, 2)?;
        let active: i64 = row.get(3)?;
        let identity: bool = row.get(4)?;
        let birth: bool = row.get(5)?;
        let proof = optional_text(row, 6)?;
        let account = optional_text(row, 7)?;
        let state: String = row.get(8)?;
        if !budget.bytes(attempt.len() + proof.as_ref().map_or(0, String::len)) {
            return Ok((None, false, CleanupState::Unknown));
        }
        let parsed = ExecutionRef::from_first_provider_attempt(id, &attempt).ok();
        let valid_account = account
            .as_deref()
            .and_then(|v| v.parse::<AccountId>().ok())
            .is_some();
        if number != expected || parsed.is_none() || !valid_account {
            complete = false;
        }
        if number == 1 && provider && valid_account {
            first = parsed;
        }
        expected = number.saturating_add(1);
        let verdict = match (phase.as_deref(), active) {
            (Some("prepared" | "spawning"), 1) => CleanupState::Pending,
            (Some("cleanup_complete"), 1) => CleanupState::Pending,
            (Some("cleanup_complete"), 0) => {
                let value = proof
                    .as_deref()
                    .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
                match value {
                    Some(value) if value.is_object() && value["never_spawned"] == true => {
                        let process: bool = conn.query_row(
                            "SELECT EXISTS(SELECT 1 FROM process_ownership WHERE owner_kind='attempt' AND owner_id=?1) OR \
                             EXISTS(SELECT 1 FROM process_members WHERE owner_kind='attempt' AND owner_id=?1)",
                            [&attempt], |row| row.get(0))?;
                        if identity
                            || birth
                            || process
                            || matches!(state.as_str(), "running" | "succeeded")
                            || value.get("confirmed").is_some_and(|v| v != true)
                            || value.get("group_gone").is_some_and(|v| v != true)
                            || value.get("descendants_gone").is_some_and(|v| v != true)
                        {
                            CleanupState::Unknown
                        } else {
                            CleanupState::Confirmed
                        }
                    }
                    Some(value)
                        if value.is_object()
                            && value["confirmed"] == true
                            && value["group_gone"] == true
                            && value["descendants_gone"] == true =>
                    {
                        CleanupState::Confirmed
                    }
                    _ => CleanupState::Unknown,
                }
            }
            _ => CleanupState::Unknown,
        };
        if verdict == CleanupState::Unknown {
            complete = false;
        }
        cleanup = merge_cleanup(cleanup, verdict);
    }
    if expected == 1 {
        complete = false;
        cleanup = CleanupState::Unknown;
    }
    Ok((first, complete, cleanup))
}

/// Classifies all retained technical-restart rows for one execution.
fn restart(
    conn: &Connection,
    root: &AgentId,
    id: &AgentId,
    sequence: u32,
    budget: &mut Budget,
) -> Result<RestartResume> {
    let mut statement = conn.prepare(
        "SELECT a.resume_status,a.cancelled_before_restart,a.resumed_execution_id,r.finished_at,r.id \
         FROM service_restart_agents a LEFT JOIN service_restarts r ON r.id=a.restart_id \
         WHERE a.agent_id=?1 ORDER BY a.restart_id LIMIT ?2")?;
    let mut rows = statement.query(params![id.as_str(), (budget.remaining_rows() + 1) as i64])?;
    let mut result = RestartResume::None;
    while let Some(row) = rows.next()? {
        if !budget.row() {
            return Ok(RestartResume::Unknown);
        }
        let state: String = row.get(0)?;
        let cancelled: i64 = row.get(1)?;
        let child: Option<String> = row.get(2)?;
        let finished: Option<f64> = row.get(3)?;
        let restart_id: Option<i64> = row.get(4)?;
        let next = if restart_id.is_none() {
            RestartResume::Unknown
        } else {
            match state.as_str() {
                "pending" if cancelled == 0 && finished.is_none() => RestartResume::Pending,
                "finished" => RestartResume::None,
                "cancelled" if cancelled == 1 => RestartResume::None,
                "started" => {
                    let linked = if let Some(child) = child {
                        conn.query_row("SELECT EXISTS(SELECT 1 FROM agents WHERE id=? AND parent_agent_id=? AND root_agent_id=? AND sequence=?)",
                        params![child,id.as_str(),root.as_str(),i64::from(sequence)+1], |row| row.get::<_,bool>(0))?
                    } else {
                        false
                    };
                    if linked {
                        RestartResume::None
                    } else {
                        RestartResume::Unknown
                    }
                }
                _ => RestartResume::Unknown,
            }
        };
        result = merge_restart(result, next);
    }
    Ok(result)
}

/// Checks only the committed database evidence for one exact successful execution.
/// The artifact is never opened and its current existence is not asserted.
/// Only a genuine SQL NULL or integer zero exit code can support recorded success.
fn recorded_success(conn: &Connection, id: &AgentId, budget: &mut Budget) -> Result<bool> {
    use std::path::{Component, Path};
    type AnswerMetadata = (
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<f64>,
        bool,
        bool,
    );
    let metadata: AnswerMetadata = conn.query_row(
        "SELECT CASE WHEN length(CAST(answer_path AS BLOB))<=?2 THEN answer_path END, \
         CASE WHEN typeof(answer_bytes)='integer' THEN answer_bytes END, \
         CASE WHEN length(CAST(answer_sha256 AS BLOB))=64 THEN answer_sha256 END, \
         CASE WHEN typeof(finished_at) IN ('real','integer') THEN finished_at END, \
         exit_code IS NULL OR (typeof(exit_code)='integer' AND exit_code=0), \
         failure_kind IS NULL AND failure_text IS NULL FROM agents WHERE id=?1",
        params![id.as_str(), MAX_PROOF_BYTES as i64],
        |row| {
            Ok((
                optional_text(row, 0)?,
                row.get(1)?,
                optional_text(row, 2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        },
    )?;
    let (Some(path), Some(bytes), Some(hash), Some(finished), valid_exit, no_failure) = metadata
    else {
        return Ok(false);
    };
    if !budget.row() || !budget.bytes(path.len() + hash.len()) || path.contains('\0') {
        return Ok(false);
    }
    let path = Path::new(&path);
    let parent = path.parent();
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
        || path.file_name().and_then(|v| v.to_str()) != Some("answer.md")
        || parent.and_then(Path::file_name).and_then(|v| v.to_str()) != Some(id.as_str())
        || parent
            .and_then(Path::parent)
            .and_then(Path::file_name)
            .and_then(|v| v.to_str())
            != Some("agents")
        || bytes <= 0
        || bytes > agent_run_platform::verify::MAX_ANSWER as i64
        || hash.parse::<agent_run_domain::Sha256Digest>().is_err()
        || !finished.is_finite()
        || finished < 0.0
        || !valid_exit
        || !no_failure
    {
        return Ok(false);
    }
    recorded_terminal_event(conn, id, Status::Succeeded, finished, Value::Null, budget)
}

/// Requires one consistent nontechnical terminal event for the exact last attempt.
fn recorded_terminal_event(
    conn: &Connection,
    id: &AgentId,
    outcome: Status,
    finished: f64,
    failure_kind: Value,
    budget: &mut Budget,
) -> Result<bool> {
    if !outcome.terminal() || !finished.is_finite() || finished < 0.0 {
        return Ok(false);
    }
    let latest_attempt: Option<(Option<String>, Option<String>, Option<f64>)> = conn
        .query_row(
            "SELECT CASE WHEN length(CAST(id AS BLOB))<=64 THEN id END, \
         CASE WHEN length(CAST(state AS BLOB))<=64 THEN state END, \
         CASE WHEN typeof(finished_at) IN ('real','integer') THEN finished_at END \
         FROM attempts WHERE agent_id=? ORDER BY number DESC LIMIT 1",
            [id.as_str()],
            |row| Ok((optional_text(row, 0)?, optional_text(row, 1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((Some(attempt), Some(state), Some(attempt_finished))) = latest_attempt else {
        return Ok(false);
    };
    if state != outcome.as_str() || !attempt_finished.is_finite() || attempt_finished != finished {
        return Ok(false);
    }
    let terminal_events: i64 = conn.query_row(
        "SELECT COUNT(*) FROM (SELECT 1 FROM events WHERE agent_id=?1 AND kind='status' \
         AND to_status IN ('succeeded','failed','timed_out','cancelled','lost') LIMIT 2)",
        [id.as_str()],
        |row| row.get(0),
    )?;
    if terminal_events != 1 {
        return Ok(false);
    }
    let mut statement = conn.prepare(
        "SELECT attempt_id,from_status,to_status,CASE WHEN length(CAST(data_json AS BLOB))<=?2 THEN data_json END \
         FROM events WHERE agent_id=?1 AND kind='status' \
         ORDER BY seq DESC LIMIT 1")?;
    let mut rows = statement.query(params![id.as_str(), MAX_PROOF_BYTES as i64])?;
    let Some(row) = rows.next()? else {
        return Ok(false);
    };
    if !budget.row() {
        return Ok(false);
    }
    let event_attempt = optional_text(row, 0)?;
    let from = optional_text(row, 1)?;
    let to = optional_text(row, 2)?;
    let data = optional_text(row, 3)?;
    let Some(data) = data else {
        return Ok(false);
    };
    if !budget.bytes(data.len()) {
        return Ok(false);
    }
    let Ok(data) = serde_json::from_str::<Value>(&data) else {
        return Ok(false);
    };
    let nontechnical = matches!(data.get("service_restart"), None | Some(Value::Bool(false)));
    let legal = from
        .as_deref()
        .and_then(|s| s.parse::<Status>().ok())
        .is_some_and(|from| from.transition(outcome).is_ok());
    Ok(data.is_object()
        && nontechnical
        && data.get("failure_kind") == Some(&failure_kind)
        && event_attempt.as_deref() == Some(attempt.as_str())
        && legal
        && to.as_deref() == Some(outcome.as_str()))
}

/// A different outcome is definitive only if its own terminal record is coherent.
fn recorded_other_outcome(
    conn: &Connection,
    id: &AgentId,
    outcome: Status,
    budget: &mut Budget,
) -> Result<bool> {
    let (finished, kind_type, kind): (Option<f64>, String, Option<String>) = conn.query_row(
        "SELECT CASE WHEN typeof(finished_at) IN ('real','integer') THEN finished_at END, typeof(failure_kind), \
         CASE WHEN length(CAST(failure_kind AS BLOB))<=?2 THEN failure_kind END FROM agents WHERE id=?1",
        params![id.as_str(),MAX_PROOF_BYTES as i64], |row| Ok((row.get(0)?,row.get(1)?,optional_text(row,2)?)),
    )?;
    let Some(finished) = finished else {
        return Ok(false);
    };
    let failure_kind = match (kind_type.as_str(), kind) {
        ("null", None) => Value::Null,
        ("text", Some(kind)) if budget.bytes(kind.len()) => Value::String(kind),
        _ => return Ok(false),
    };
    recorded_terminal_event(conn, id, outcome, finished, failure_kind, budget)
}

/// Streams a lineage and retains at most the requested positions and its tail.
fn lineage(
    conn: &Connection,
    root: &AgentId,
    wanted: &HashSet<u32>,
    budget: &mut Budget,
) -> Result<Lineage> {
    if budget.remaining_rows() == 0 {
        return Ok(Lineage::unknown());
    }
    let mut statement = conn.prepare(
        "SELECT id,root_agent_id,sequence,parent_agent_id,status FROM agents \
         WHERE root_agent_id=?1 OR id=?1 ORDER BY sequence,id LIMIT ?2",
    )?;
    let mut rows = statement.query(params![root.as_str(), (budget.remaining_rows() + 1) as i64])?;
    let mut result = Lineage {
        selected: HashMap::new(),
        tail: None,
        structural: true,
        cleanup: CleanupState::Confirmed,
        restart: RestartResume::None,
    };
    let mut previous: Option<(AgentId, u32, Status)> = None;
    let mut last_duplicate = false;
    while let Some(row) = rows.next()? {
        if !budget.row() {
            return Ok(Lineage::unknown());
        }
        let raw_id: String = row.get(0)?;
        let raw_root: String = row.get(1)?;
        let raw_sequence: i64 = row.get(2)?;
        let parent: Option<String> = row.get(3)?;
        let status: String = row.get(4)?;
        let (Ok(id), Ok(sequence), Ok(raw_status)) = (
            raw_id.parse::<AgentId>(),
            u32::try_from(raw_sequence),
            status.parse::<Status>(),
        ) else {
            return Ok(Lineage::unknown());
        };
        if sequence == 0 || raw_root != root.as_str() {
            result.structural = false;
        }
        let chain_ok = match &previous {
            None => sequence == 1 && id == *root && parent.is_none(),
            Some((previous_id, previous_sequence, previous_status)) => {
                previous_status.terminal()
                    && previous_sequence.checked_add(1) == Some(sequence)
                    && parent.as_deref() == Some(previous_id.as_str())
            }
        };
        if !chain_ok {
            result.structural = false;
        }
        last_duplicate = previous
            .as_ref()
            .is_some_and(|(_, prev, _)| *prev == sequence);
        let provider = provider_identity(conn, &id, budget)?;
        let (execution_ref, attempts_complete, cleanup) = attempts(conn, &id, provider, budget)?;
        if !provider || !attempts_complete {
            result.structural = false;
        }
        result.cleanup = merge_cleanup(result.cleanup, cleanup);
        result.restart = merge_restart(result.restart, restart(conn, root, &id, sequence, budget)?);
        let execution = Execution {
            id: id.clone(),
            fact: ExecutionFact {
                sequence,
                execution_ref,
                raw_status,
            },
        };
        if wanted.contains(&sequence) {
            match result.selected.entry(sequence) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.insert(None);
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(Some(execution.clone()));
                }
            }
        }
        result.tail = Some(execution);
        previous = Some((id, sequence, raw_status));
    }
    drop(rows);
    drop(statement);
    if let Some((tail, _, _)) = &previous {
        let foreign_child: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM agents WHERE parent_agent_id=?)",
            [tail.as_str()],
            |row| row.get(0),
        )?;
        if foreign_child {
            result.structural = false;
        }
    } else {
        result.structural = false;
        result.cleanup = CleanupState::Unknown;
    }
    if last_duplicate {
        result.tail = None;
    }
    if budget.exhausted {
        result.structural = false;
        result.cleanup = CleanupState::Unknown;
        result.restart = RestartResume::Unknown;
    }
    Ok(result)
}

/// Reads all requested facts using the caller's already-open SQLite snapshot.
///
/// The caller may project an AgentView using the same connection before ending
/// the transaction. No async work or OS process observation occurs here.
pub fn read_in_snapshot<'a>(
    conn: &'a Connection,
    request: &StatusRequest,
) -> Result<StatusSnapshot<'a>> {
    request.validate()?;
    if conn.is_autocommit() {
        return Err(invalid("status requires a read snapshot"));
    }
    let store_id = crate::store_identity::require(conn, request.expected_store_id.as_ref())?;
    let observed_at = now();
    let mut canonical = Vec::with_capacity(request.targets.len());
    let mut seen = HashSet::new();
    let mut wanted: HashMap<AgentId, HashSet<u32>> = HashMap::new();
    for target in &request.targets {
        let row: Option<Option<String>> = conn
            .query_row(
                "SELECT COALESCE(NULLIF(root_agent_id,''),id) FROM agents WHERE id=?",
                [target.agent_id.as_str()],
                |row| optional_text(row, 0),
            )
            .optional()?;
        let (root, malformed_root) = match row {
            Some(Some(root)) => match root.parse::<AgentId>() {
                Ok(root) => (Some(root), false),
                Err(_) => (None, true),
            },
            Some(None) => (None, true),
            None => {
                let found: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM agents WHERE root_agent_id=?)",
                    [target.agent_id.as_str()],
                    |row| row.get(0),
                )?;
                (found.then(|| target.agent_id.clone()), false)
            }
        };
        let key = (
            root.as_ref().unwrap_or(&target.agent_id).clone(),
            target.sequence,
        );
        if !seen.insert(key.clone()) {
            return Err(invalid("duplicate canonical status target"));
        }
        if let Some(root) = &root {
            wanted
                .entry(root.clone())
                .or_default()
                .insert(target.sequence);
        }
        canonical.push((root, malformed_root));
    }
    let mut budget = Budget::default();
    let mut cache = HashMap::new();
    for root in canonical.iter().filter_map(|(root, _)| root.as_ref()) {
        if !cache.contains_key(root) {
            cache.insert(
                root.clone(),
                lineage(conn, root, &wanted[root], &mut budget)?,
            );
        }
    }
    let mut items = Vec::with_capacity(request.targets.len());
    let mut selected = Vec::with_capacity(request.targets.len());
    let mut latest = Vec::with_capacity(request.targets.len());
    for (selector, (root, malformed_root)) in request.targets.iter().zip(canonical) {
        let mut item = StatusItem {
            selector: selector.clone(),
            canonical_agent_id: root.clone(),
            r#match: if malformed_root {
                StatusMatch::Unknown
            } else {
                StatusMatch::Missing
            },
            pin_id: selector.pin_id.clone(),
            pin_state: if selector.pin_id.is_none() {
                PinState::Missing
            } else {
                PinState::Unknown
            },
            selected: None,
            retained_latest: None,
            coverage: Coverage::Unknown,
            restart_resume: RestartResume::Unknown,
            lineage_terminal: None,
            terminal_outcome: None,
            success_commit: SuccessCommit::Unknown,
            cleanup: CleanupFact {
                scope: "lineage".into(),
                state: CleanupState::Unknown,
            },
        };
        let mut selected_id = None;
        let mut latest_id = None;
        if let Some(root) = root {
            let lineage = &cache[&root];
            if let Some(pin) = &selector.pin_id {
                let pin_row: Option<(String, i64)> = conn
                    .query_row(
                        "SELECT root_agent_id,released FROM history_pins WHERE pin_id=?",
                        [pin.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                item.pin_state = match pin_row {
                    None => PinState::Missing,
                    Some((pin_root, _)) if pin_root != root.as_str() => PinState::Mismatch,
                    Some((_, 0)) => PinState::Active,
                    Some((_, 1)) => PinState::Released,
                    _ => PinState::Unknown,
                };
            }
            item.coverage = if item.pin_state == PinState::Active && lineage.structural {
                Coverage::Complete
            } else {
                Coverage::Unknown
            };
            item.restart_resume = match (lineage.restart, item.coverage) {
                (RestartResume::None, Coverage::Unknown) => RestartResume::Unknown,
                (state, _) => state,
            };
            match lineage.selected.get(&selector.sequence) {
                None if lineage.tail.is_some() => item.r#match = StatusMatch::Missing,
                None => item.r#match = StatusMatch::Unknown,
                Some(None) => item.r#match = StatusMatch::Unknown,
                Some(Some(execution)) => {
                    item.r#match = match (&selector.execution_ref, &execution.fact.execution_ref) {
                        (None, _) => StatusMatch::Unverified,
                        (Some(expected), Some(actual)) if expected == actual => {
                            StatusMatch::Verified
                        }
                        (Some(_), Some(_)) => StatusMatch::Mismatch,
                        (Some(_), None) => StatusMatch::Unknown,
                    };
                    if item.r#match != StatusMatch::Mismatch {
                        item.selected = Some(execution.fact.clone());
                        selected_id = Some(execution.id.clone());
                    }
                }
            }
            if matches!(
                item.r#match,
                StatusMatch::Missing | StatusMatch::Mismatch | StatusMatch::Unknown
            ) {
                item.coverage = Coverage::Unknown;
                item.restart_resume = RestartResume::Unknown;
            }
            if matches!(
                item.r#match,
                StatusMatch::Verified | StatusMatch::Unverified
            ) && let Some(tail) = &lineage.tail
            {
                item.retained_latest = Some(tail.fact.clone());
                latest_id = Some(tail.id.clone());
                item.lineage_terminal = match (item.coverage, item.restart_resume) {
                    (_, RestartResume::Pending) => Some(false),
                    (Coverage::Complete, RestartResume::None)
                        if item.r#match == StatusMatch::Verified =>
                    {
                        Some(tail.fact.raw_status.terminal())
                    }
                    _ => None,
                };
                if item.lineage_terminal == Some(true) {
                    item.terminal_outcome = Some(tail.fact.raw_status);
                    item.success_commit = if tail.fact.raw_status != Status::Succeeded {
                        if recorded_other_outcome(
                            conn,
                            &tail.id,
                            tail.fact.raw_status,
                            &mut budget,
                        )? {
                            SuccessCommit::NotSucceeded
                        } else {
                            SuccessCommit::Unknown
                        }
                    } else if recorded_success(conn, &tail.id, &mut budget)? {
                        SuccessCommit::Recorded
                    } else {
                        SuccessCommit::Unknown
                    };
                }
                item.cleanup.state = if item.r#match == StatusMatch::Verified
                    && item.coverage == Coverage::Complete
                {
                    match (item.restart_resume, item.lineage_terminal, lineage.cleanup) {
                        (RestartResume::Unknown, _, _) | (_, _, CleanupState::Unknown) => {
                            CleanupState::Unknown
                        }
                        (RestartResume::Pending, _, _)
                        | (_, Some(false), _)
                        | (_, _, CleanupState::Pending) => CleanupState::Pending,
                        (_, Some(true), CleanupState::Confirmed) => CleanupState::Confirmed,
                        _ => CleanupState::Unknown,
                    }
                } else {
                    CleanupState::Unknown
                };
            }
        }
        items.push(item);
        selected.push(selected_id);
        latest.push(latest_id);
    }
    Ok(StatusSnapshot {
        response: StatusResponse {
            version: 1,
            store_id,
            observed_at,
            items,
        },
        selected,
        latest,
        snapshot: std::marker::PhantomData,
    })
}

impl Store<'_> {
    /// Runs the same proof under an owned read transaction and closes it before returning.
    pub fn status_snapshot(&self, request: &StatusRequest) -> Result<StatusResponse> {
        let tx = self.conn.unchecked_transaction()?;
        let result = read_in_snapshot(&tx, request)?.response;
        tx.commit()?;
        Ok(result)
    }
}
