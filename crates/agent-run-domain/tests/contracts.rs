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

// --- provider orchestration catalog contracts (schema v17 preparation) ---

use agent_run_domain::catalog::{
    decode_legacy_request, legacy_runtime, AccountId, AccountRecord, AccountStatus,
    AttemptCredentials, AuthFamily, HarnessId, PhysicalQuotaKey, ProviderBinding, ProviderCatalog,
    ProviderDefinition, ProviderId, ProviderModel, QuotaAdmissionError, QuotaCandidate,
    QuotaCandidateSet, ResolvedLaunchAuthority, SecretHandle, SecretRef, SelectionIntent,
};

/// Builds one registered account bound by two provider aliases.
fn aliased_catalog() -> ProviderCatalog {
    let account = AccountId::from_str("acct-codex-native").unwrap();
    let model = ProviderModel {
        id: "gpt-5.1".into(),
        native_model: None,
        params: [("effort".to_string(), "medium|high".to_string())].into(),
        recommendations: vec!["general coding".into()],
        restrictions: vec![],
    };
    let make = |id: &str| ProviderDefinition {
        id: ProviderId::from_str(id).unwrap(),
        harness: HarnessId::Codex,
        protocol_endpoint: "https://api.example.com".into(),
        auth_family: AuthFamily::from_str("openai").unwrap(),
        models: vec![model.clone()],
        bindings: vec![ProviderBinding {
            label: "plus".into(),
            account: account.clone(),
            models: None,
            multiplier: PositiveFinite::try_from(1.0).unwrap(),
        }],
    };
    ProviderCatalog::new(
        vec![AccountRecord {
            account_id: account.clone(),
            auth_family: AuthFamily::from_str("openai").unwrap(),
            secret_ref: SecretRef::from_str("keychain:codex").unwrap(),
            status: AccountStatus::Enabled,
        }],
        vec![make("codex-plus"), make("codex-pro")],
    )
    .unwrap()
}

/// The same global account under two provider aliases is one physical pool:
/// identical quota keys, both aliases listed, and capacity is never summed by
/// adding aliases.
#[test]
fn provider_alias_identity_shares_one_physical_quota_pool() {
    let catalog = aliased_catalog();
    let account = AccountId::from_str("acct-codex-native").unwrap();
    let plus = PhysicalQuotaKey::new(&account, "gpt-5.1").unwrap();
    let pro = PhysicalQuotaKey::new(&account, "gpt-5.1").unwrap();
    assert_eq!(plus, pro);
    assert_eq!(plus.as_str(), "acct-codex-native::gpt-5.1");
    assert_ne!(
        PhysicalQuotaKey::new(&account, "gpt-5.1").unwrap(),
        PhysicalQuotaKey::new(&account, "gpt-5.2").unwrap()
    );
    assert_eq!(catalog.aliases_of(&account).len(), 2);
    // A different account is a different pool even on the same lane.
    let other = AccountId::from_str("acct-claude-native").unwrap();
    assert_ne!(PhysicalQuotaKey::new(&other, "gpt-5.1").unwrap(), plus);
}

/// Auth-family mismatches and unregistered accounts are rejected at catalog
/// construction; the binding never silently widens eligibility.
#[test]
fn provider_binding_requires_registered_matching_auth_family() {
    let account = AccountId::from_str("acct-claude-native").unwrap();
    let mut wrong = aliased_catalog().providers()[0].clone();
    wrong.bindings[0].account = account.clone();
    let unregistered = ProviderCatalog::new(vec![], vec![wrong.clone()]);
    assert!(matches!(
        unregistered,
        Err(agent_run_domain::Error::Validation(_))
    ));
    let mismatched = ProviderCatalog::new(
        vec![AccountRecord {
            account_id: account,
            auth_family: AuthFamily::from_str("anthropic").unwrap(),
            secret_ref: SecretRef::from_str("keychain:claude").unwrap(),
            status: AccountStatus::Enabled,
        }],
        vec![wrong],
    );
    assert!(mismatched.is_err());
}

/// Auto and pinned intents are distinct, persisted with the candidate set, and
/// a pinned intent must appear among the ordered candidates.
#[test]
fn quota_candidate_set_preserves_pinned_and_auto_intent() {
    let account = AccountId::from_str("acct-codex-native").unwrap();
    let provider = ProviderId::from_str("codex-plus").unwrap();
    let candidate = QuotaCandidate {
        account: account.clone(),
        multiplier: PositiveFinite::try_from(1.0).unwrap(),
    };
    let mut set = QuotaCandidateSet {
        provider: provider.clone(),
        model: "gpt-5.1".into(),
        intent: SelectionIntent::Pinned(account.clone()),
        candidates: vec![candidate],
        capacity_revision: 7,
    };
    set.validate().unwrap();
    let wire = serde_json::to_value(&set).unwrap();
    assert_eq!(wire["intent"]["pinned"], "acct-codex-native");
    let round: QuotaCandidateSet = serde_json::from_value(wire).unwrap();
    assert_eq!(round, set);

    set.intent = SelectionIntent::Auto;
    set.validate().unwrap();
    // Auto with zero candidates is structurally valid; emptiness is the
    // admission verdict no_eligible_account, not a DTO validation error.
    set.candidates.clear();
    set.validate().unwrap();
    set.intent = SelectionIntent::Pinned(account);
    let missing_pin = set.validate().unwrap_err();
    assert!(missing_pin.to_string().contains("pinned"));
}

/// Stale revision, busy, no-eligible-account, and exhaustion are distinct
/// typed verdicts with stable machine names that never masquerade as each
/// other.
#[test]
fn quota_admission_verdicts_are_typed_and_stable() {
    let stale = QuotaAdmissionError::SelectionStale {
        committed_capacity_revision: 7,
        current_capacity_revision: 9,
    };
    assert_eq!(stale.kind(), "selection_stale");
    assert!(stale.to_string().contains("selection_stale"));
    let busy = QuotaAdmissionError::SelectionBusy { stale_retries: 3 };
    assert_eq!(busy.kind(), "selection_busy");
    let none = QuotaAdmissionError::NoEligibleAccount {
        provider: ProviderId::from_str("codex-plus").unwrap(),
        model: "gpt-5.1".into(),
    };
    assert_eq!(none.kind(), "no_eligible_account");
    let exhausted = QuotaAdmissionError::QuotaExhausted {
        provider: ProviderId::from_str("codex-plus").unwrap(),
        model: "gpt-5.1".into(),
        account: AccountId::from_str("acct-codex-native").unwrap(),
    };
    assert_eq!(exhausted.kind(), "quota_exhausted");
    let wire = serde_json::to_value(&stale).unwrap();
    assert_eq!(
        serde_json::from_value::<QuotaAdmissionError>(wire).unwrap(),
        stale
    );
}

/// The immutable launch authority serializes on the wire while the per-attempt
/// credential lease deliberately does not: `SecretHandle` implements no serde
/// trait, so secret references cannot cross a wire or log boundary.
#[test]
fn launch_authority_is_serializable_but_credential_leases_are_not() {
    let account = AccountId::from_str("acct-codex-native").unwrap();
    let authority = ResolvedLaunchAuthority {
        provider: ProviderId::from_str("codex-plus").unwrap(),
        harness: HarnessId::Codex,
        protocol_endpoint: "https://api.example.com".into(),
        model: "gpt-5.1".into(),
        effort: Some("high".into()),
        profile: "engineer".into(),
        workdir: "/tmp".into(),
        grants: Default::default(),
        assets_sha256: None,
        eligible_accounts: vec![account.clone()],
    };
    authority.validate().unwrap();
    let wire = serde_json::to_value(&authority).unwrap();
    assert_eq!(wire["model"], "gpt-5.1");
    // No selected account field exists on the authority.
    assert!(wire.get("selected_account").is_none());
    assert!(wire.get("credentials").is_none());

    let lease = AttemptCredentials {
        secret: SecretHandle::from_reference("keychain:codex").unwrap(),
        account,
    };
    assert_eq!(lease.secret.reference(), "keychain:codex");
    // SecretHandle is not Serialize/Deserialize by construction; this test
    // compiles only because the assertions below never serialize the lease.
    assert_eq!(lease.account.as_str(), "acct-codex-native");
}

/// Historical request_json decodes into current contracts without rewriting
/// the stored blob, and unknown runtimes are a typed refusal, never a guess.
#[test]
fn legacy_request_json_decodes_without_rewriting_history() {
    let stored = json!({
        "runtime": "codex",
        "model": "gpt-5.1",
        "profile": "engineer",
        "task": "do the thing",
        "workdir": "/tmp",
        "account": "personal2"
    });
    let decoded = decode_legacy_request(&stored).unwrap();
    assert_eq!(decoded.provider.as_str(), "codex");
    assert_eq!(decoded.harness, HarnessId::Codex);
    assert_eq!(decoded.request.model, "gpt-5.1");
    assert_eq!(decoded.account.as_ref().unwrap().as_str(), "personal2");
    // The input value is untouched.
    assert_eq!(stored["runtime"], "codex");

    let claude = decode_legacy_request(&json!({
        "runtime": "claude", "model": "m", "profile": "p", "task": "t", "workdir": "/tmp"
    }))
    .unwrap();
    assert_eq!(claude.harness, HarnessId::ClaudeCode);
    assert_eq!(
        legacy_runtime("opencode").unwrap_err().to_string(),
        "unknown historical runtime \"opencode\"; it has no provider mapping"
    );
}
