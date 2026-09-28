//! Shared test fixtures for reducer and rendering tests.

use agent_run_domain::AgentView;
use serde_json::json;

/// Builds one session view from a sparse fixture with valid identities.
///
/// Ids must already be valid stable ids (`ag-YYYYMMDD-HHMMSS-<10 lowercase hex>`).
pub fn agent_view(stable_id: &str, run_id: &str, status: &str) -> AgentView {
    serde_json::from_value(json!({
        "agent_id": stable_id,
        "run_id": run_id,
        "runtime": "codex",
        "model": "gpt-5",
        "profile": "default",
        "task_summary": "ship the thing",
        "status": status,
        "created_at": 1.0,
        "started_at": 1.0,
        "finished_at": None::<f64>,
        "elapsed_seconds": 42.0,
        "last_progress_at": None::<f64>,
        "silence_seconds": Some(3.0),
        "warned": false,
        "failure_kind": None::<String>,
        "failure_text": None::<String>,
        "answer_available": false,
        "answer_bytes": None::<u64>,
        "answer_sha256": None::<String>,
        "effort": None::<String>,
        "delivery": {
            "agent_id": stable_id,
            "bound": false,
            "orchestrator_session_id": None::<String>,
            "notification_id": None::<String>,
            "state": "idle",
            "attempts": 0,
            "ambiguous": false,
            "last_error": None::<String>,
            "last_attempt": None::<serde_json::Value>,
        },
        "parent_agent_id": None::<String>,
        "root_agent_id": stable_id,
        "sequence": 1,
        "cleanup": None::<serde_json::Value>,
        "policy": None::<serde_json::Value>,
        "phase": "running",
        "phase_started_at": 1.0,
        "process_state": "alive",
        "observed_at": 1.0,
        "runtime_outcome": None::<String>,
        "acceptance": "pending",
    }))
    .expect("fixture parses")
}

/// Builds one transcript message fixture.
pub fn message(seq: i64, role: &str, content: &str) -> agent_run_domain::MessageView {
    serde_json::from_value(json!({
        "seq": seq,
        "at": 100.0 + seq as f64,
        "role": role,
        "name": None::<String>,
        "content": content,
        "raw_ref": None::<String>,
    }))
    .expect("message fixture parses")
}
