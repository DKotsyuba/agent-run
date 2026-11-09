//! Read-only exact initial task of a retained root execution, one SQLite snapshot per page.

use agent_run_domain::{
    AccountId, Error, Result, StoreId,
    batch::ExecutionRef,
    domain::{AgentId, task_text},
    error::invalid,
    initial_task::{
        InitialTaskMode, InitialTaskPage, InitialTaskReason, InitialTaskRequest,
        InitialTaskResponse, InitialTaskUnavailable, MAX_TASK_BYTES, VERSION,
    },
    status::{Coverage, PinState, StatusMatch, StatusRequest, StatusSelector},
};
use rusqlite::{Connection, OptionalExtension, params};

/// A source row far above the admission bound is unreadable evidence, not a task.
const MAX_REQUEST_JSON_BYTES: i64 = 16 * 1024 * 1024;
/// Leaves 8 KiB for the bounded JSON-RPC ID and MCP structured-result envelope.
const MAX_WIRE_BYTES: usize = agent_run_domain::observation::SOCKET_MAX_FRAME_BYTES - 8192;

/// Types and sizes of the three task sources, inspected before any text is loaded.
struct SourceShape {
    task: Option<i64>,
    request_json: Option<i64>,
    request_task: Option<i64>,
}

/// Reads one page inside a fresh read transaction.
pub fn read(conn: &Connection, request: &InitialTaskRequest) -> Result<InitialTaskResponse> {
    request.validate()?;
    let tx = conn.unchecked_transaction()?;
    let result = read_in_snapshot(&tx, request)?;
    tx.commit()?;
    Ok(result)
}

/// Caller-owned snapshot seam; store, root, ref, sources and page bytes share it.
pub fn read_in_snapshot(
    conn: &Connection,
    request: &InitialTaskRequest,
) -> Result<InitialTaskResponse> {
    request.validate()?;
    if conn.is_autocommit() {
        return Err(invalid("initial task requires a read snapshot"));
    }
    let store_id = crate::store_identity::require(conn, request.expected_store_id.as_ref())?;
    let refuse = |reason| unavailable(request, &store_id, reason);
    let strict = request.mode == InitialTaskMode::Strict;

    // Canonical root/self: an alias or any later execution never exports its own task.
    let root: Option<(Option<String>, Option<i64>, bool)> = conn
        .query_row(
            "SELECT COALESCE(NULLIF(root_agent_id,''),id),sequence,parent_agent_id IS NULL \
             FROM agents WHERE id=?",
            [request.agent_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((root_id, sequence, no_parent)) = root else {
        // A released pin outlives its deleted lineage, so it can name retention as the cause.
        let mut tombstone = false;
        if let (true, Some(pin)) = (strict, &request.pin_id) {
            tombstone = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM history_pins WHERE pin_id=?1 AND root_agent_id=?2)",
                params![pin.as_str(), request.agent_id.as_str()],
                |row| row.get::<_, bool>(0),
            )?;
        }
        return Ok(refuse(if tombstone {
            InitialTaskReason::RetentionDeleted
        } else {
            InitialTaskReason::NotFound
        }));
    };
    if root_id.as_deref() != Some(request.agent_id.as_str())
        || sequence != Some(1)
        || !no_parent
        || request.sequence() != 1
    {
        return Ok(refuse(InitialTaskReason::NotRootExecution));
    }

    // First provider attempt and its computed ref; no later attempt identifies the execution.
    let first: Option<(Option<String>, Option<String>)> = conn
        .query_row(
            "SELECT CASE WHEN typeof(id)='text' AND length(CAST(id AS BLOB))<=64 THEN id END, \
             CASE WHEN typeof(selected_account_id)='text' AND length(CAST(selected_account_id AS BLOB))<=256 \
             THEN selected_account_id END FROM attempts WHERE agent_id=? AND number=1",
            [request.agent_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let execution_ref = match first {
        Some((Some(attempt), Some(account))) if account.parse::<AccountId>().is_ok() => {
            ExecutionRef::from_first_provider_attempt(&request.agent_id, &attempt).ok()
        }
        _ => None,
    };
    let Some(execution_ref) = execution_ref else {
        return Ok(refuse(InitialTaskReason::ExecutionRefUnavailable));
    };
    if request
        .execution_ref
        .as_ref()
        .is_some_and(|expected| expected != &execution_ref)
    {
        return Ok(refuse(InitialTaskReason::ExecutionRefMismatch));
    }

    // Three-way agreement on the exact text, sized before any of it is loaded.
    let shape = source_shape(conn, &request.agent_id)?;
    let (Some(task_len), Some(_), Some(request_len)) =
        (shape.task, shape.request_json, shape.request_task)
    else {
        return Ok(refuse(InitialTaskReason::TaskMissing));
    };
    if task_len > MAX_TASK_BYTES as i64 || request_len > MAX_TASK_BYTES as i64 {
        return Ok(refuse(InitialTaskReason::TaskTooLarge));
    }
    let (task, request_task): (Option<String>, Option<String>) = conn.query_row(
        "SELECT task,json_extract(request_json,'$.task') FROM agents WHERE id=?",
        [request.agent_id.as_str()],
        |row| Ok((utf8(row, 0)?, utf8(row, 1)?)),
    )?;
    let (Some(task), Some(request_task)) = (task, request_task) else {
        return Ok(refuse(InitialTaskReason::TaskMissing));
    };
    if task_text(&task).is_err() {
        return Ok(refuse(InitialTaskReason::TaskMissing));
    }
    if task != request_task {
        return Ok(refuse(InitialTaskReason::TaskMismatch));
    }
    let Some(frozen) = crate::status::verified_provider_request(conn, &request.agent_id)? else {
        return Ok(refuse(InitialTaskReason::ProviderIdentityInvalid));
    };
    if frozen.task != task {
        return Ok(refuse(InitialTaskReason::TaskMismatch));
    }

    // Strict proof, after the cheap content checks: the existing status evidence must
    // verify the same ref, an active pin of this root and complete lineage/attempt
    // coverage in this very snapshot.
    if strict {
        let status = crate::status::read_in_snapshot(
            conn,
            &StatusRequest {
                version: 1,
                expected_store_id: request.expected_store_id.clone(),
                targets: vec![StatusSelector {
                    agent_id: request.agent_id.clone(),
                    sequence: 1,
                    execution_ref: request.execution_ref.clone(),
                    pin_id: request.pin_id.clone(),
                }],
                include_view: false,
            },
        )?;
        let item = &status.response.items[0];
        let pinned = match item.pin_state {
            PinState::Active => None,
            PinState::Missing => Some(InitialTaskReason::PinMissing),
            PinState::Released => Some(InitialTaskReason::PinReleased),
            PinState::Mismatch => Some(InitialTaskReason::PinMismatch),
            PinState::Unknown => Some(InitialTaskReason::CoverageIncomplete),
        };
        if let Some(reason) = pinned {
            return Ok(refuse(reason));
        }
        if item.r#match != StatusMatch::Verified
            || item.coverage != Coverage::Complete
            || status.selected_execution(0) != Some(&request.agent_id)
        {
            return Ok(refuse(InitialTaskReason::CoverageIncomplete));
        }
    }

    let bytes = task.as_bytes();
    let sha256 = agent_run_platform::fs::sha256(bytes).parse::<agent_run_domain::Sha256Digest>()?;
    if request
        .expected_sha256
        .as_ref()
        .is_some_and(|expected| expected != &sha256)
        || request
            .expected_total_bytes
            .is_some_and(|expected| expected != bytes.len())
    {
        return Ok(refuse(InitialTaskReason::ContentChanged));
    }
    let start = request.offset;
    if start >= task.len() || !task.is_char_boundary(start) {
        return Err(invalid(
            "initial task offset must be a UTF-8 boundary within the text",
        ));
    }
    let mut end = start.saturating_add(request.max_bytes).min(task.len());
    while !task.is_char_boundary(end) {
        end -= 1;
    }
    let complete = end == task.len();
    let response = InitialTaskResponse::Available(InitialTaskPage {
        version: VERSION,
        mode: request.mode,
        store_id: store_id.clone(),
        agent_id: request.agent_id.clone(),
        sequence: 1,
        execution_ref,
        pin_id: if strict { request.pin_id.clone() } else { None },
        total_bytes: task.len(),
        sha256,
        offset: start,
        end,
        next_offset: (!complete).then_some(end),
        text: task[start..end].to_owned(),
        complete,
    });
    response.validate_for(request)?;
    if serde_json::to_vec(&response)?.len() > MAX_WIRE_BYTES {
        return Err(Error::Runtime(
            "initial task page exceeds frame budget".into(),
        ));
    }
    Ok(response)
}

/// Typed refusal carrying no content.
fn unavailable(
    request: &InitialTaskRequest,
    store_id: &StoreId,
    reason: InitialTaskReason,
) -> InitialTaskResponse {
    InitialTaskResponse::Unavailable(InitialTaskUnavailable {
        version: VERSION,
        mode: request.mode,
        store_id: store_id.clone(),
        agent_id: request.agent_id.clone(),
        reason,
    })
}

/// Text column, or `None` when it is not valid UTF-8 text.
fn utf8(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<String>> {
    Ok(match row.get_ref(index)? {
        rusqlite::types::ValueRef::Text(bytes) => {
            std::str::from_utf8(bytes).ok().map(str::to_owned)
        }
        _ => None,
    })
}

/// Byte lengths of text-typed sources; `None` marks absent, non-text or unreadable evidence.
fn source_shape(conn: &Connection, id: &AgentId) -> Result<SourceShape> {
    Ok(conn.query_row(
        "SELECT CASE WHEN typeof(task)='text' THEN length(CAST(task AS BLOB)) END, \
         CASE WHEN typeof(request_json)='text' AND length(CAST(request_json AS BLOB))<=?2 \
         THEN length(CAST(request_json AS BLOB)) END, \
         CASE WHEN typeof(request_json)='text' AND length(CAST(request_json AS BLOB))<=?2 \
         AND json_valid(request_json) AND json_type(request_json,'$.task')='text' \
         THEN length(CAST(json_extract(request_json,'$.task') AS BLOB)) END \
         FROM agents WHERE id=?1",
        params![id.as_str(), MAX_REQUEST_JSON_BYTES],
        |row| {
            Ok(SourceShape {
                task: row.get(0)?,
                request_json: row.get(1)?,
                request_task: row.get(2)?,
            })
        },
    )?)
}
