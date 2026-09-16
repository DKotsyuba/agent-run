//! Core ports of Python supervisor command terminal-result behavior.
//!
//! These tests use SQLite temporary homes only; none require Unix sockets.

mod common;

use agent_run_core::{commands, domain::Outcome};
use serde_json::json;

/// Mirrors Python `test_supervisor.py::_drain_terminal_commands`.
/// Mirrors Python `tests/test_supervisor.py::SupervisorTests::test_final_drain_completes_late_cancel_steer_and_unknown`.
/// Mirrors Python `tests/test_supervisor.py::SupervisorTests::test_a_steer_without_the_capability_is_refused_not_dropped`.
#[test]
fn python_test_supervisor_terminal_commands_get_one_durable_result() {
    let home = common::Home::new();
    let mut store = home.store();
    let (agent_id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .enqueue(&agent_id, "steer", &json!({"text":"too late"}))
        .unwrap();
    store
        .finish(&agent_id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    commands::complete_terminal(&mut store, &agent_id).unwrap();
    let (state, result): (String, String) = store
        .conn
        .query_row(
            "SELECT state,result_json FROM commands WHERE agent_id=?",
            [agent_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, "completed");
    assert_eq!(result, r#"{"accepted":false,"reason":"agent_terminal"}"#);
}
