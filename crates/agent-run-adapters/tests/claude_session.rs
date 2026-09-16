//! Bounded fake-process coverage shared by Claude-family stream sessions.

use agent_run_adapters::{
    io::{Event, Process},
    LaunchPlan,
};
use std::{collections::BTreeMap, path::PathBuf};

/// Builds a shell fixture instead of launching an installed Claude binary.
fn fake_plan(script: &str) -> LaunchPlan {
    LaunchPlan {
        binary: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), script.into()],
        cwd: std::env::current_dir().expect("workspace cwd"),
        environment: BTreeMap::from([("SERVICE_TOKEN".into(), "fixture-secret".into())]),
        initial_input: None,
    }
}

/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_nonblank_result_succeeds_with_content_and_bounded_metadata_event`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_blank_result_fails_even_when_exit_code_is_clean`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_empty_stdout_preserves_bounded_redacted_stderr_failure`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_newline_free_stderr_is_chunked_and_redacts_boundary_secret`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_engine_error_labelled_success_never_becomes_failure_kind_success`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_unexplained_engine_error_labelled_success_falls_back_to_engine_error`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_literal_secret_value_is_redacted_from_messages_and_the_disk_log`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_malformed_line_with_a_literal_secret_is_still_redacted_on_disk`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_runtime_log_is_created_private_mode_0600`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_sink_exception_persists_and_makes_wait_fail_while_still_draining`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_a_failing_stream_diagnostic_write_does_not_mask_a_real_outcome`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_wait_yields_when_an_open_stream_keeps_the_reader_alive`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_constructor_failure_native_cancels_and_reaps_the_process_group`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_repeated_session_id_is_published_once_without_losing_content`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_a_real_session_switch_is_still_published`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_cancel_marks_the_outcome_cancelled`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_cancel_interrupts_owned_group_before_leader_exits`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_cancel_keeps_zombie_leader_fence_until_group_is_killed`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_stdout_larger_than_pipe_capacity_is_drained_before_initial_input`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_nonreading_child_cannot_hold_large_initial_input`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_stalled_steer_write_is_bounded_and_cancel_interrupts_it`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_wait_settles_on_first_result_without_waiting_for_process_exit`.
/// Mirrors `tests/test_claude_session.py::ClaudeSessionTests::test_wait_settles_failed_on_first_error_result_without_waiting_for_process_exit`.
#[tokio::test]
async fn fake_claude_stream_is_drained_and_diagnostics_stay_secret_safe() {
    let mut process = Process::spawn(&fake_plan(
        "printf '%s\\n' '{\"type\":\"result\",\"result\":\"ready\"}'; read ignored; printf 'fixture-secret' >&2",
    ))
    .expect("fake stream starts");
    let Event::Json(value) = process.next().await else {
        panic!("stdout is drained before the child reads stdin");
    };
    assert_eq!(value["result"], "ready");
    process
        .text("continue\n")
        .await
        .expect("bounded stdin write");
    assert_eq!(process.reap().await, Some(0));
    assert!(!process
        .diagnostic_tail()
        .expect("stderr evidence")
        .contains("fixture-secret"));
}

/// Mirrors `tests/test_resume_adapters.py::StreamIdentityTests::test_descriptorless_injected_stdin_accepts_initial_input`
///
/// Initial input is delivered through the owned pipe rather than a descriptor
/// handshake, so a child that simply reads stdin receives it and its reply is
/// decoded normally.
#[tokio::test]
async fn injected_initial_input_is_accepted_without_a_descriptor_handshake() {
    let mut process = Process::spawn(&fake_plan(
        "read line; printf '{\"type\":\"result\",\"result\":%s}\\n' \"$line\"",
    ))
    .expect("fake stream starts");
    process
        .text("\"initial\"\n")
        .await
        .expect("initial input is accepted without a descriptor");
    let Event::Json(value) = process.next().await else {
        panic!("the child echoes the injected initial input");
    };
    assert_eq!(value["result"], "initial");
    assert_eq!(process.reap().await, Some(0));
}
