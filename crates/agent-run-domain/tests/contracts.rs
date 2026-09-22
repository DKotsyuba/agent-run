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

/// Credential references identify existing protected stores without accepting
/// inline tokens, relative files, or escaping named-account labels.
#[test]
fn credential_references_are_typed_storage_locations() {
    for reference in [
        "native:codex",
        "named:claude-code:work",
        "env:FAKE_TOKEN",
        "file:/tmp/fake-auth",
        "keychain:fake.service:FAKE_KEY",
    ] {
        assert!(
            reference.parse::<agent_run_domain::CredentialRef>().is_ok(),
            "{reference}"
        );
    }
    for reference in [
        "fake-raw-token",
        "named:codex:../escape",
        "file:relative",
        "env:bad",
        "keychain:missing",
    ] {
        assert!(
            reference
                .parse::<agent_run_domain::CredentialRef>()
                .is_err(),
            "{reference}"
        );
    }
    assert_eq!(
        format!("{:?}", "fake-raw-token".parse::<SecretRef>().unwrap()),
        "SecretRef(<redacted>)"
    );
}

use agent_run_domain::catalog::{
    decode_legacy_request, legacy_runtime, AccountId, AccountRecord, AccountStatus,
    AttemptCredentials, AuthFamily, HarnessId, LegacyRuntime, LimitsSource,
    NormalizedQuotaSnapshot, PhysicalQuotaKey, ProviderBinding, ProviderCatalog,
    ProviderConnection, ProviderDefinition, ProviderId, ProviderModel, ProviderProtocol,
    QuotaAdmissionError, QuotaCandidate, QuotaCandidateSet, QuotaModelObservation,
    QuotaPoolObservation, QuotaWindow, ResolvedLaunchAuthority, SecretRef, SelectionIntent,
};

/// Builds one registered account bound by two provider aliases.
fn aliased_catalog() -> ProviderCatalog {
    let account = AccountId::from_str("acct-codex-native").unwrap();
    let model = ProviderModel {
        id: "gpt-5.1".into(),
        native_model: None,
        params: [("effort".to_string(), "medium|high".to_string())].into(),
        allowed_params: Default::default(),
        recommendations: vec!["general coding".into()],
        restrictions: vec![],
    };
    let make = |id: &str| ProviderDefinition {
        id: ProviderId::from_str(id).unwrap(),
        harness: HarnessId::Codex,
        connection: ProviderConnection::Custom {
            endpoint: "https://api.example.com".into(),
            protocol: ProviderProtocol::Responses,
            auth_header: Default::default(),
            allow_loopback_http: false,
        },
        auth_family: AuthFamily::from_str("openai").unwrap(),
        recommendations: vec![],
        priority_multiplier: PositiveFinite::try_from(1.0).unwrap(),
        limits_source: LimitsSource::CodexAppserver,
        collector: None,
        models: vec![model.clone()],
        bindings: vec![ProviderBinding {
            label: "plus".parse().unwrap(),
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
    assert!(serde_json::from_value::<PhysicalQuotaKey>(json!("acct-codex-native::")).is_err());
    assert!(serde_json::from_value::<PhysicalQuotaKey>(json!("not-a-key")).is_err());
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

/// Catalog decoding and construction share validation for labels, duplicate
/// identities, model subsets, endpoints, and account storage identities.
#[test]
fn catalog_wire_and_endpoint_reject_invalid_registration() {
    assert!(serde_json::from_value::<ProviderBinding>(json!({
        "label":"../outside", "account":"acct-codex-native"
    }))
    .is_err());
    let valid = aliased_catalog();
    let mut wire = serde_json::to_value(&valid).unwrap();
    wire["providers"][1]["id"] = json!("codex-plus");
    assert!(serde_json::from_value::<ProviderCatalog>(wire).is_err());
    let mut wire = serde_json::to_value(&valid).unwrap();
    let duplicate = wire["accounts"][0].clone();
    wire["accounts"].as_array_mut().unwrap().push(duplicate);
    assert!(serde_json::from_value::<ProviderCatalog>(wire).is_err());
    let mut wire = serde_json::to_value(&valid).unwrap();
    wire["providers"][0]["bindings"][0]["account"] = json!("acct-missing");
    assert!(serde_json::from_value::<ProviderCatalog>(wire).is_err());
    let mut wire = serde_json::to_value(&valid).unwrap();
    wire["providers"][0]["bindings"][0]["models"] = json!(["missing"]);
    assert!(serde_json::from_value::<ProviderCatalog>(wire).is_err());
    let mut wire = serde_json::to_value(&valid).unwrap();
    wire["providers"][0]["auth_family"] = json!("anthropic");
    assert!(serde_json::from_value::<ProviderCatalog>(wire).is_err());

    let mut provider = valid.providers()[0].clone();
    provider.bindings[0].models = Some(vec!["missing".into()]);
    assert!(ProviderCatalog::new(
        vec![valid
            .account(&"acct-codex-native".parse().unwrap())
            .unwrap()
            .clone()],
        vec![provider]
    )
    .is_err());

    let mut provider = valid.providers()[0].clone();
    for endpoint in [
        "",
        "http://example.com",
        "file:///tmp/x",
        "https://user:pass@example.com",
    ] {
        provider.connection = ProviderConnection::Custom {
            endpoint: endpoint.into(),
            protocol: ProviderProtocol::Responses,
            auth_header: Default::default(),
            allow_loopback_http: false,
        };
        assert!(provider.validate().is_err(), "{endpoint}");
    }
    provider.connection = ProviderConnection::Custom {
        endpoint: "http://localhost:8080".into(),
        protocol: ProviderProtocol::Responses,
        auth_header: Default::default(),
        allow_loopback_http: false,
    };
    assert!(provider.validate().is_err());
    provider.connection = ProviderConnection::Custom {
        endpoint: "http://localhost:8080".into(),
        protocol: ProviderProtocol::Responses,
        auth_header: Default::default(),
        allow_loopback_http: true,
    };
    provider.validate().unwrap();
    provider.connection = ProviderConnection::Custom {
        endpoint: "http://[::1]:8080".into(),
        protocol: ProviderProtocol::Responses,
        auth_header: Default::default(),
        allow_loopback_http: true,
    };
    provider.validate().unwrap();
    provider.connection = ProviderConnection::Custom {
        endpoint: "http://example.com".into(),
        protocol: ProviderProtocol::Responses,
        auth_header: Default::default(),
        allow_loopback_http: true,
    };
    assert!(provider.validate().is_err());
    provider.connection = ProviderConnection::Native;
    provider.auth_family = "anthropic".parse().unwrap();
    assert!(provider.validate().is_err());

    let original = valid
        .account(&"acct-codex-native".parse().unwrap())
        .unwrap()
        .clone();
    let mut duplicate = original.clone();
    duplicate.account_id = "acct-other".parse().unwrap();
    assert!(ProviderCatalog::new(vec![original, duplicate], vec![]).is_err());
    assert!(" keychain:codex".parse::<SecretRef>().is_err());
}

/// Every alias in one provider is returned, while a lease can only be minted
/// from the selected account's own enabled catalog record and model scope.
#[test]
fn aliases_and_lease_scope_remain_bound_to_one_account() {
    let mut provider = aliased_catalog().providers()[0].clone();
    provider.bindings.push(ProviderBinding {
        label: "backup".parse().unwrap(),
        account: "acct-codex-native".parse().unwrap(),
        models: None,
        multiplier: PositiveFinite::try_from(1.0).unwrap(),
    });
    let record = aliased_catalog()
        .account(&"acct-codex-native".parse().unwrap())
        .unwrap()
        .clone();
    let catalog = ProviderCatalog::new(vec![record], vec![provider]).unwrap();
    let account: AccountId = "acct-codex-native".parse().unwrap();
    assert_eq!(
        catalog
            .aliases_of(&account)
            .iter()
            .map(|(_, label)| *label)
            .collect::<Vec<_>>(),
        vec!["plus", "backup"]
    );
    assert!(AttemptCredentials::from_selected(
        &catalog,
        &"missing".parse().unwrap(),
        "gpt-5.1",
        &account
    )
    .is_err());
    assert!(AttemptCredentials::from_selected(
        &catalog,
        &"codex-plus".parse().unwrap(),
        "missing",
        &account
    )
    .is_err());
    assert!(AttemptCredentials::from_selected(
        &catalog,
        &"codex-plus".parse().unwrap(),
        "gpt-5.1",
        &"acct-other".parse().unwrap()
    )
    .is_err());
}

/// Collector observations preserve missing values and reject cross-account
/// keys, invalid percentages, stale timing, and duplicate model membership.
#[test]
fn normalized_quota_snapshot_validates_physical_membership() {
    let account: AccountId = "acct-codex-native".parse().unwrap();
    let key = PhysicalQuotaKey::new(&account, "tokens").unwrap();
    let mut snapshot = NormalizedQuotaSnapshot {
        account: account.clone(),
        models: vec![QuotaModelObservation {
            model: "gpt-5.1".into(),
            pools: vec![QuotaPoolObservation {
                key,
                windows: vec![QuotaWindow {
                    source: "provider".into(),
                    name: "5h".into(),
                    remaining_percent: None,
                    reset_at: None,
                    observed_at: 1.0,
                    valid_until: 2.0,
                }],
            }],
        }],
    };
    snapshot.validate().unwrap();
    snapshot.models[0].pools.push(QuotaPoolObservation {
        key: PhysicalQuotaKey::new(&account, "requests").unwrap(),
        windows: vec![],
    });
    snapshot.validate().unwrap();
    snapshot.models[0].pools.push(QuotaPoolObservation {
        key: PhysicalQuotaKey::new(&"acct-other".parse().unwrap(), "tokens").unwrap(),
        windows: vec![],
    });
    assert!(snapshot.validate().is_err());
    snapshot.models[0].pools.pop();
    snapshot.models[0].pools[0].windows[0].remaining_percent = Some(101.0);
    assert!(snapshot.validate().is_err());
}

/// Shared physical pools must carry one canonical observation across models;
/// window order is immaterial, while missing or contradictory facts are not.
#[test]
fn shared_quota_pool_observations_agree_across_models() {
    let account: AccountId = "acct".parse().unwrap();
    let pool = QuotaPoolObservation {
        key: PhysicalQuotaKey::new(&account, "shared").unwrap(),
        windows: vec![
            QuotaWindow {
                source: "provider".into(),
                name: "5h".into(),
                remaining_percent: Some(90.0),
                reset_at: None,
                observed_at: 1.0,
                valid_until: 2.0,
            },
            QuotaWindow {
                source: "provider".into(),
                name: "7d".into(),
                remaining_percent: None,
                reset_at: Some(5.0),
                observed_at: 1.0,
                valid_until: 2.0,
            },
        ],
    };
    let mut snapshot = NormalizedQuotaSnapshot {
        account,
        models: vec![
            QuotaModelObservation {
                model: "m1".into(),
                pools: vec![pool.clone()],
            },
            QuotaModelObservation {
                model: "m2".into(),
                pools: vec![pool.clone()],
            },
        ],
    };
    snapshot.models[1].pools[0].windows.reverse();
    snapshot.validate().unwrap();
    snapshot.models[1].pools[0].windows[1].remaining_percent = Some(5.0);
    assert!(snapshot.validate().is_err());
    snapshot.models[1].pools[0].windows = vec![pool.windows[0].clone()];
    assert!(snapshot.validate().is_err());
    snapshot.models[1].pools[0].windows = pool.windows.clone();
    snapshot.models[1].pools[0].windows[1].remaining_percent = Some(1.0);
    assert!(snapshot.validate().is_err());
    snapshot.models[1].pools[0].windows = pool.windows.clone();
    snapshot.models[1].pools[0].windows[0].reset_at = Some(3.0);
    assert!(snapshot.validate().is_err());
    snapshot.models[1].pools[0].windows = pool.windows.clone();
    snapshot.models[1].pools[0].windows[0].observed_at = 1.5;
    assert!(snapshot.validate().is_err());
    snapshot.models[1].pools[0].windows = pool.windows.clone();
    snapshot.models[1].pools[0].windows[0].valid_until = 3.0;
    assert!(snapshot.validate().is_err());
}

/// Auto and pinned intents are distinct, persisted with the candidate set, and
/// a pinned intent must appear among the ordered candidates.
#[test]
fn quota_candidate_set_preserves_pinned_and_auto_intent() {
    let account = AccountId::from_str("acct-codex-native").unwrap();
    let provider = ProviderId::from_str("codex-plus").unwrap();
    let candidate = QuotaCandidate {
        account: account.clone(),
        rank: 0,
        physical_keys: vec![PhysicalQuotaKey::new(&account, "gpt-5.1").unwrap()],
        multiplier: PositiveFinite::try_from(1.0).unwrap(),
        quota_known: true,
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
    set.candidates[0]
        .physical_keys
        .push(PhysicalQuotaKey::new(&"acct-other".parse().unwrap(), "gpt-5.1").unwrap());
    assert!(set.validate().is_err());
    set.candidates[0].physical_keys.pop();

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

/// Producer rank groups must be monotone; admission may compare active counts
/// only after rank equality and never interpret a rank as a quota percentage.
#[test]
fn quota_candidate_rank_groups_are_ordered() {
    let first: AccountId = "acct-first".parse().unwrap();
    let second: AccountId = "acct-second".parse().unwrap();
    let mut set = QuotaCandidateSet {
        provider: "custom".parse().unwrap(),
        model: "m".into(),
        intent: SelectionIntent::Auto,
        candidates: [(&first, 0), (&second, 1)]
            .into_iter()
            .map(|(account, rank)| QuotaCandidate {
                account: account.clone(),
                rank,
                physical_keys: vec![PhysicalQuotaKey::new(account, "shared").unwrap()],
                multiplier: PositiveFinite::try_from(1.0).unwrap(),
                quota_known: true,
            })
            .collect(),
        capacity_revision: 0,
    };
    set.validate().unwrap();
    set.candidates[1].rank = 0;
    set.validate().unwrap();
    set.candidates[0].rank = 2;
    assert!(set.validate().is_err());
}

/// New provider input has one explicit model and rejects legacy runtime or
/// caller-supplied quota candidates before any service/store side effect.
#[test]
fn provider_start_request_is_strict_and_validated() {
    let workdir = std::env::temp_dir().canonicalize().unwrap();
    let input = json!({
        "provider":"glm-user", "model":"fixture", "profile":"review",
        "task":"inspect", "workdir":workdir, "account":"work"
    });
    let mut request: agent_run_domain::ProviderStartRequest =
        serde_json::from_value(input.clone()).unwrap();
    request.validate().unwrap();
    assert_eq!(request.storage_projection().runtime, "glm-user");
    for extra in ["runtime", "candidates", "quota_snapshot"] {
        let mut invalid = input.clone();
        invalid[extra] = json!("untrusted");
        assert!(serde_json::from_value::<agent_run_domain::ProviderStartRequest>(invalid).is_err());
    }
    let mut blank = request.clone();
    blank.model = " ".into();
    assert!(blank.validate().is_err());
    let mut invalid_label = input;
    invalid_label["account"] = json!("../outside");
    assert!(
        serde_json::from_value::<agent_run_domain::ProviderStartRequest>(invalid_label).is_err()
    );
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
    let mut role = json!({
        "role_name": "engineer",
        "prompt": "do work",
        "grants": {"write": true, "network": false, "read_roots": ["/tmp"]},
        "skills": [], "mcp": [], "required_constraints": []
    });
    let revision = agent_run_domain::canonical::sha256_hex(&role, true);
    role["config_revision"] = json!(revision);
    let mut authority = ResolvedLaunchAuthority {
        provider: ProviderId::from_str("codex-plus").unwrap(),
        harness: HarnessId::Codex,
        connection: ProviderConnection::Native,
        model: "gpt-5.1".into(),
        effort: Some("high".into()),
        profile: "engineer".into(),
        workdir: "/tmp".into(),
        role_payload: role,
        assets_sha256: "a".repeat(64).parse().unwrap(),
        eligible_accounts: vec![account.clone()],
    };
    authority.validate().unwrap();
    let wire = serde_json::to_value(&authority).unwrap();
    assert_eq!(wire["model"], "gpt-5.1");
    // No selected account field exists on the authority.
    assert!(wire.get("selected_account").is_none());
    assert!(wire.get("credentials").is_none());

    let catalog = aliased_catalog();
    let lease = AttemptCredentials::from_selected(
        &catalog,
        &"codex-plus".parse().unwrap(),
        "gpt-5.1",
        &account,
    )
    .unwrap();
    assert_eq!(lease.secret().reference(), "keychain:codex");
    assert_eq!(format!("{:?}", lease.secret()), "SecretHandle(<redacted>)");
    // SecretHandle is not Serialize/Deserialize by construction; this test
    // compiles only because the assertions below never serialize the lease.
    assert_eq!(lease.account().as_str(), "acct-codex-native");
    authority.role_payload["grants"]["write"] = json!(false);
    assert!(authority.validate().is_err());
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
    let codex = LegacyRuntime {
        provider: "codex".parse().unwrap(),
        harness: HarnessId::Codex,
    };
    let decoded = decode_legacy_request(&stored, Some(&codex)).unwrap();
    assert_eq!(decoded.provider.as_ref().unwrap().as_str(), "codex");
    assert_eq!(decoded.harness, Some(HarnessId::Codex));
    assert_eq!(decoded.request.model, "gpt-5.1");
    assert_eq!(decoded.account.as_ref().unwrap().as_str(), "personal2");
    // The input value is untouched.
    assert_eq!(stored["runtime"], "codex");

    let claude = decode_legacy_request(
        &json!({
            "runtime": "claude", "model": "m", "profile": "p", "task": "t", "workdir": "/tmp"
        }),
        Some(&LegacyRuntime {
            provider: "claude-code".parse().unwrap(),
            harness: HarnessId::ClaudeCode,
        }),
    )
    .unwrap();
    assert_eq!(claude.harness, Some(HarnessId::ClaudeCode));
    assert_eq!(legacy_runtime("opencode"), None);
    for (runtime, provider, harness) in [
        ("glm", "glm", HarnessId::ClaudeCode),
        ("main", "codex", HarnessId::Codex),
        ("main", "claude-code", HarnessId::ClaudeCode),
    ] {
        let raw =
            json!({"runtime":runtime, "model":"m", "profile":"p", "task":"t", "workdir":"/tmp"});
        let evidence = LegacyRuntime {
            provider: provider.parse().unwrap(),
            harness,
        };
        let decoded = decode_legacy_request(&raw, Some(&evidence)).unwrap();
        assert_eq!(decoded.request.runtime, runtime);
        assert_eq!(decoded.provider.unwrap().as_str(), provider);
        assert_eq!(decode_legacy_request(&raw, None).unwrap().provider, None);
    }
    assert!(decode_legacy_request(
        &stored,
        Some(&LegacyRuntime {
            provider: "claude-code".parse().unwrap(),
            harness: HarnessId::ClaudeCode
        })
    )
    .is_err());
}
