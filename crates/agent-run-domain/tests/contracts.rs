//! Cross-language domain contracts mirrored from Python's `tests/test_domain.py`.

use agent_run_domain::{
    domain::{AgentId, Status},
    error::{Error, MachineCode},
    fsm::{validate_transition, ACTIVE, TERMINAL},
    types::{
        AbsoluteDirectory, AccountSelector, Message, MessageRole, NonNegativeFinite,
        PositiveFinite, RelativeOwnedPath, RuntimeName, Sha256Digest,
    },
    views::{AgentPage, AgentView, DeliveryView},
};
use serde_json::json;
use std::{path::PathBuf, str::FromStr};

/// Mirrors Python `DomainTests.test_state_machine_matches_frozen_contract`.
#[test]
fn python_test_state_machine_matches_frozen_contract() {
    assert_eq!(ACTIVE.len() + TERMINAL.len(), Status::ALL.len());
    assert!(Status::Succeeded.terminal());
    assert!(!Status::Running.terminal());
    validate_transition(Status::Created, Status::Starting).unwrap();
    assert!(matches!(
        validate_transition(Status::Succeeded, Status::Running),
        Err(Error::Transition(_))
    ));
    assert_eq!(Status::TimedOut.as_str(), "timed_out");
}

/// Mirrors Python `DomainTests.test_agent_id_generation_and_validation`.
#[test]
fn python_test_agent_id_generation_and_validation() {
    let generated = AgentId::new();
    assert_eq!(generated.as_str().parse::<AgentId>().unwrap(), generated);
    for invalid in [
        "",
        "../outside",
        "ag-20260230-010203-0123456789",
        "ag-20260825-010203-012345678G",
    ] {
        assert!(invalid.parse::<AgentId>().is_err(), "{invalid:?}");
    }
}

/// Mirrors Python `DomainTests.test_message_and_outcome_validation`.
#[test]
fn python_test_message_validation_and_nonfinite_rejection() {
    Message {
        at: 0.0,
        role: MessageRole::User,
        content: "hello".into(),
        name: None,
        raw_ref: None,
    }
    .validate()
    .unwrap();
    assert!(Message {
        at: -1.0,
        role: MessageRole::User,
        content: "hello".into(),
        name: None,
        raw_ref: None
    }
    .validate()
    .is_err());
    assert!(Message {
        at: 0.0,
        role: MessageRole::User,
        content: "  ".into(),
        name: None,
        raw_ref: None
    }
    .validate()
    .is_err());
    assert!(PositiveFinite::try_from(f64::INFINITY).is_err());
    assert!(NonNegativeFinite::try_from(f64::NAN).is_err());
}

/// Mirrors Python account selector semantics: an absent account is native global, never a default label.
#[test]
fn python_account_absence_is_global_not_default_label() {
    assert!(matches!(
        AccountSelector::from_wire(None).unwrap(),
        AccountSelector::Global(_)
    ));
    assert_eq!(
        AccountSelector::from_wire(Some("personal2"))
            .unwrap()
            .as_wire(),
        Some("personal2")
    );
    assert!(AccountSelector::from_wire(Some("Personal")).is_err());
}

/// Mirrors Python path validation categories in `DomainTests.test_start_request_validates_paths_timeout_and_duplicate_roots`.
#[test]
fn python_path_and_digest_newtypes_reject_escape_and_invalid_evidence() {
    let root = std::env::temp_dir();
    assert!(AbsoluteDirectory::try_from(root).is_ok());
    assert!(RelativeOwnedPath::try_from(PathBuf::from("answer.md")).is_ok());
    assert!(RelativeOwnedPath::try_from(PathBuf::from("../answer.md")).is_err());
    assert!(Sha256Digest::from_str(&"a".repeat(64)).is_ok());
    assert!(Sha256Digest::from_str(&"A".repeat(64)).is_err());
    assert!(RuntimeName::from_str("codex_appserver").is_ok());
}

/// Mirrors Python socket/MCP error-mapping cases: validation is `-32602`; expected domain errors are `-32000`.
#[test]
fn python_transport_error_mapping_preserves_answer_integrity() {
    let validation = Error::Validation("bad input".into());
    assert_eq!(validation.machine_code(), MachineCode::ValidationError);
    assert_eq!(validation.protocol_mapping().json_rpc_code, -32602);
    let integrity = Error::AnswerIntegrity("proof mismatch".into());
    assert_eq!(integrity.public().kind, "AnswerIntegrityError");
    assert_eq!(integrity.protocol_mapping().json_rpc_code, -32000);
    assert_eq!(integrity.protocol_mapping().cli_exit_code, 2);
}

/// Mirrors Python `_jsonable` dataclass ordering and null emission for agent pages.
#[test]
fn python_view_dtos_keep_field_order_and_nulls() {
    let id: AgentId = "ag-20260825-010203-0123456789".parse().unwrap();
    let view = AgentView {
        agent_id: id.clone(),
        runtime: "codex".into(),
        model: "model".into(),
        profile: "profile".into(),
        task_summary: "task".into(),
        status: Status::Starting,
        created_at: 1.0,
        started_at: None,
        finished_at: None,
        elapsed_seconds: 0.0,
        last_progress_at: None,
        silence_seconds: None,
        warned: false,
        failure_kind: None,
        failure_text: None,
        answer_available: false,
        answer_bytes: None,
        answer_sha256: None,
        effort: None,
        delivery: DeliveryView {
            agent_id: id.clone(),
            bound: false,
            orchestrator_session_id: None,
            notification_id: None,
            state: "not_created".into(),
            attempts: 0,
            ambiguous: false,
            last_error: None,
            last_attempt: None,
        },
        parent_agent_id: None,
        root_agent_id: Some(id.clone()),
        sequence: 1,
        cleanup: None,
        policy: None,
        phase: "accepted".into(),
        phase_started_at: 1.0,
        process_state: "not_started".into(),
        observed_at: 1.0,
        runtime_outcome: None,
        acceptance: "pending".into(),
    };
    let page = AgentPage {
        items: vec![view],
        total: 1,
        offset: 0,
        limit: 100,
        next_offset: None,
        complete: true,
        revision: 0,
        observed_at: 1.0,
    };
    let value = serde_json::to_value(page).unwrap();
    assert_eq!(value["items"][0]["started_at"], json!(null));
    let keys: Vec<_> = value["items"][0].as_object().unwrap().keys().collect();
    assert_eq!(
        keys[..4],
        ["acceptance", "agent_id", "answer_available", "answer_bytes"]
    );
}
