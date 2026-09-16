//! Ports the completion-decision-policy behavior of `tests/test_verify.py`
//! (Python) to Rust's `verify::{inspect_answer, silence_seconds, verify_completion}`.
//! Each test names the Python test it mirrors.
mod common;
use agent_run_domain::domain::{Outcome, Status};
use agent_run_platform::verify::{
    self, ANSWER_INCOMPLETE, ANSWER_PRESENT, ENGINE_VANISHED, GROUP_SURVIVED, NO_ANSWER,
};
use std::path::Path;

const SENTINEL: &str = "<<<agent-run:complete>>>";

fn write(root: &Path, text: &str) -> std::path::PathBuf {
    let path = root.join("answer.md");
    std::fs::write(&path, text).unwrap();
    path
}

// --- inspect_answer: mirrors test_verify.py::InspectAnswerTests ---

/// Mirrors `test_missing_file_is_no_answer`.
#[test]
fn inspect_missing_file_is_no_answer() {
    let h = common::Home::new();
    let proof = verify::inspect_answer(&h.path, Path::new("absent.md")).unwrap();
    assert!(!proof.exists);
    assert!(!proof.complete());
    assert!(proof.sha256.is_none());
    assert_eq!(proof.evidence(), NO_ANSWER);
}

/// Mirrors `test_empty_file_is_no_answer`.
#[test]
fn inspect_empty_file_is_no_answer() {
    let h = common::Home::new();
    write(&h.path, "");
    let proof = verify::inspect_answer(&h.path, Path::new("answer.md")).unwrap();
    assert!(proof.exists);
    assert_eq!(proof.size_bytes, 0);
    assert_eq!(proof.evidence(), NO_ANSWER);
}

/// Mirrors `test_answer_without_sentinel_is_cut_off`.
#[test]
fn inspect_answer_without_sentinel_is_cut_off() {
    let h = common::Home::new();
    write(&h.path, "half a thought");
    let proof = verify::inspect_answer(&h.path, Path::new("answer.md")).unwrap();
    assert!(proof.exists);
    assert!(!proof.sentinel_found);
    assert!(!proof.complete());
    assert_eq!(proof.evidence(), ANSWER_INCOMPLETE);
}

/// Mirrors `test_sentinel_makes_the_answer_complete_and_hashed`.
#[test]
fn inspect_sentinel_makes_the_answer_complete_and_hashed() {
    let h = common::Home::new();
    let body = format!("done\n{SENTINEL}\n");
    let path = write(&h.path, &body);
    let proof = verify::inspect_answer(&h.path, Path::new("answer.md")).unwrap();
    assert!(proof.complete());
    assert_eq!(proof.evidence(), ANSWER_PRESENT);
    assert_eq!(proof.size_bytes, body.len() as u64);
    assert_eq!(
        proof.sha256.as_deref(),
        Some(agent_run_platform::fs::sha256(body.as_bytes()).as_str())
    );
    assert_eq!(proof.path, path);
}

/// Mirrors `test_sentinel_split_across_a_read_boundary_is_found`. Rust reads
/// the whole file in one shot rather than in chunks, so there is no separate
/// boundary-splitting code path to exercise — this just confirms a large
/// payload still finds the terminal frame.
#[test]
fn inspect_sentinel_in_a_large_payload_is_found() {
    let h = common::Home::new();
    let frame = format!("\n{SENTINEL}\n");
    let head = "x".repeat(65536 - frame.len() / 2);
    write(&h.path, &(head + &frame));
    let proof = verify::inspect_answer(&h.path, Path::new("answer.md")).unwrap();
    assert!(proof.sentinel_found);
}

/// Mirrors `test_sentinel_mentioned_in_truncated_prose_is_incomplete`: do not
/// accept marker text unless it is the exact terminal frame.
#[test]
fn inspect_sentinel_mentioned_in_prose_is_incomplete() {
    let h = common::Home::new();
    write(&h.path, &format!("mentioned {SENTINEL} then cut off"));
    let proof = verify::inspect_answer(&h.path, Path::new("answer.md")).unwrap();
    assert!(!proof.complete());
    assert!(!proof.sentinel_found);
}

/// Mirrors `test_oversized_answer_is_rejected_before_streaming`.
#[test]
fn inspect_oversized_answer_is_rejected_before_streaming() {
    let h = common::Home::new();
    let path = h.path.join("answer.md");
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(verify::MAX_ANSWER as u64 + 1).unwrap();
    assert!(verify::inspect_answer(&h.path, Path::new("answer.md")).is_err());
}

/// Mirrors `tests/test_verify.py::InspectAnswerTests::test_blank_sentinel_is_refused`
#[test]
fn inspect_blank_sentinel_is_refused() {
    let h = common::Home::new();
    write(&h.path, "body");
    assert!(
        verify::inspect_answer_with_sentinel(&h.path, Path::new("answer.md"), Some("   ")).is_err()
    );
}

/// Mirrors `tests/test_verify.py::InspectAnswerTests::test_no_sentinel_required_accepts_any_nonempty_answer`
#[test]
fn inspect_no_sentinel_required_accepts_any_nonempty_answer() {
    let h = common::Home::new();
    write(&h.path, "free form");
    let proof =
        verify::inspect_answer_with_sentinel(&h.path, Path::new("answer.md"), None).unwrap();
    assert!(proof.complete());
    write(&h.path, "");
    let empty =
        verify::inspect_answer_with_sentinel(&h.path, Path::new("answer.md"), None).unwrap();
    assert_eq!(empty.evidence(), NO_ANSWER);
}

// --- silence_seconds: mirrors test_verify.py::SilenceTests ---

/// Mirrors `test_silence_is_measured_from_the_last_progress`.
#[test]
fn silence_is_measured_from_the_last_progress() {
    assert_eq!(verify::silence_seconds(None, 10.0).unwrap(), None);
    assert_eq!(verify::silence_seconds(Some(4.0), 10.0).unwrap(), Some(6.0));
    assert_eq!(
        verify::silence_seconds(Some(12.0), 10.0).unwrap(),
        Some(0.0)
    );
}

/// Mirrors `test_non_finite_progress_is_refused`.
#[test]
fn non_finite_progress_is_refused() {
    assert!(verify::silence_seconds(Some(f64::NAN), 1.0).is_err());
}

// --- verify_completion: mirrors test_verify.py::VerifyCompletionTests ---

fn proof(root: &Path, text: Option<&str>) -> verify::AnswerProof {
    let path = root.join("answer.md");
    if let Some(text) = text {
        std::fs::write(&path, text).unwrap();
    }
    verify::inspect_answer(root, Path::new("answer.md")).unwrap()
}

/// Mirrors `test_a_surviving_group_is_never_reported_as_finished`.
#[test]
fn a_surviving_group_is_never_reported_as_finished() {
    let h = common::Home::new();
    let answer = proof(&h.path, Some(&format!("ok\n{SENTINEL}\n")));
    let outcome = verify::verify_completion(
        Some(Outcome::success(None)),
        None,
        Some(&answer),
        false,
        None,
        0.0,
        60.0,
    )
    .unwrap();
    assert_eq!(outcome.status, Status::Failed);
    assert_eq!(outcome.failure_kind.as_deref(), Some(GROUP_SURVIVED));
    assert!(outcome.failure_text.unwrap().contains(ANSWER_PRESENT));
}

/// Mirrors `test_cancel_records_the_answer_evidence_it_had`.
#[test]
fn cancel_records_the_answer_evidence_it_had() {
    let h = common::Home::new();
    let answer = proof(&h.path, Some("partial"));
    let outcome = verify::verify_completion(
        None,
        Some(verify::StopReason::Cancel),
        Some(&answer),
        true,
        Some(1.0),
        4.0,
        10.0,
    )
    .unwrap();
    assert_eq!(outcome.status, Status::Cancelled);
    assert_eq!(outcome.failure_kind.as_deref(), Some(ANSWER_INCOMPLETE));
    assert_eq!(outcome.failure_text.as_deref(), Some("silence=3.0s/active"));
}

/// Mirrors `test_cancel_and_timeout_preserve_a_complete_answer`. Rust's
/// `Outcome` carries no `answer_*` fields (see `verify::completion` module
/// docs) — the caller already holds the `AnswerProof`/`Proof` it built, so
/// this checks that value directly instead of round-tripping it through
/// `Outcome`.
#[test]
fn cancel_and_timeout_preserve_a_complete_answer() {
    let h = common::Home::new();
    let body = format!("usable partial result\n{SENTINEL}\n");
    let answer = proof(&h.path, Some(&body));
    for (reason, status) in [
        (verify::StopReason::Cancel, Status::Cancelled),
        (verify::StopReason::Timeout, Status::TimedOut),
    ] {
        let outcome = verify::verify_completion(
            Some(Outcome::success(Some("sess-stop".into()))),
            Some(reason),
            Some(&answer),
            true,
            None,
            0.0,
            60.0,
        )
        .unwrap();
        assert_eq!(outcome.status, status);
        assert_eq!(outcome.runtime_session_id.as_deref(), Some("sess-stop"));
        assert!(answer.complete());
        assert_eq!(answer.size_bytes, body.len() as u64);
    }
}

/// Mirrors `test_timeout_without_any_answer_reports_silence`.
#[test]
fn timeout_without_any_answer_reports_silence() {
    let h = common::Home::new();
    let answer = proof(&h.path, None);
    let outcome = verify::verify_completion(
        None,
        Some(verify::StopReason::Timeout),
        Some(&answer),
        true,
        None,
        90.0,
        60.0,
    )
    .unwrap();
    assert_eq!(outcome.status, Status::TimedOut);
    assert_eq!(outcome.failure_kind.as_deref(), Some(NO_ANSWER));
    assert_eq!(outcome.failure_text.as_deref(), Some("silence=no_progress"));
}

/// Mirrors `tests/test_verify.py::VerifyCompletionTests::test_unknown_stop_reason_is_refused`
#[test]
fn unknown_stop_reason_is_refused() {
    let h = common::Home::new();
    let answer = proof(&h.path, Some("partial"));
    assert!(verify::verify_completion_with_stop_reason(
        None,
        Some("unknown"),
        Some(&answer),
        true,
        None,
        0.0,
        60.0,
    )
    .is_err());
}

/// Mirrors `test_timeout_with_a_silent_engine_says_silent`.
#[test]
fn timeout_with_a_silent_engine_says_silent() {
    let h = common::Home::new();
    let answer = proof(&h.path, Some("cut off"));
    let outcome = verify::verify_completion(
        None,
        Some(verify::StopReason::Timeout),
        Some(&answer),
        true,
        Some(10.0),
        200.0,
        60.0,
    )
    .unwrap();
    assert_eq!(outcome.failure_kind.as_deref(), Some(ANSWER_INCOMPLETE));
    assert_eq!(
        outcome.failure_text.as_deref(),
        Some("silence=190.0s/silent")
    );
}

/// Mirrors `test_engine_success_without_a_complete_answer_is_a_failure`.
#[test]
fn engine_success_without_a_complete_answer_is_a_failure() {
    let h = common::Home::new();
    let answer = proof(&h.path, Some("no marker"));
    let mut success = Outcome::success(None);
    success.exit_code = Some(0);
    let outcome =
        verify::verify_completion(Some(success), None, Some(&answer), true, None, 0.0, 60.0)
            .unwrap();
    assert_eq!(outcome.status, Status::Failed);
    assert_eq!(outcome.failure_kind.as_deref(), Some(ANSWER_INCOMPLETE));
    assert_eq!(outcome.exit_code, Some(0));
}

/// Mirrors `test_success_with_a_complete_answer_carries_the_proof` (adapted:
/// see `cancel_and_timeout_preserve_a_complete_answer` docs above).
#[test]
fn success_with_a_complete_answer_passes_through() {
    let h = common::Home::new();
    let body = format!("the answer\n{SENTINEL}\n");
    let answer = proof(&h.path, Some(&body));
    let outcome = verify::verify_completion(
        Some(Outcome::success(Some("sess-1".into()))),
        None,
        Some(&answer),
        true,
        None,
        0.0,
        60.0,
    )
    .unwrap();
    assert_eq!(outcome.status, Status::Succeeded);
    assert_eq!(outcome.runtime_session_id.as_deref(), Some("sess-1"));
    assert_eq!(
        answer.sha256,
        Some(agent_run_platform::fs::sha256(body.as_bytes()))
    );
    assert_eq!(answer.size_bytes, body.len() as u64);
}

/// Mirrors `test_a_vanished_engine_is_a_failure`.
#[test]
fn a_vanished_engine_is_a_failure() {
    let h = common::Home::new();
    let answer = proof(&h.path, None);
    let outcome =
        verify::verify_completion(None, None, Some(&answer), true, None, 0.0, 60.0).unwrap();
    assert_eq!(outcome.status, Status::Failed);
    assert_eq!(outcome.failure_kind.as_deref(), Some(ENGINE_VANISHED));
}

/// Mirrors `test_an_engine_failure_is_passed_through`.
#[test]
fn an_engine_failure_is_passed_through() {
    let h = common::Home::new();
    let answer = proof(&h.path, None);
    let mut failure = Outcome::failure("engine_error");
    failure.exit_code = Some(2);
    let outcome = verify::verify_completion(
        Some(failure.clone()),
        None,
        Some(&answer),
        true,
        None,
        0.0,
        60.0,
    )
    .unwrap();
    assert_eq!(outcome.status, failure.status);
    assert_eq!(outcome.failure_kind, failure.failure_kind);
    assert_eq!(outcome.exit_code, failure.exit_code);
}

// Python's `test_unknown_stop_reason_is_refused` has no Rust equivalent:
// `StopReason` is a two-variant enum, so an invalid third stop reason is not
// a representable value in the first place.
