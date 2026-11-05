//! Delivery-only Bridge SDK adapter; no native caller or execution authority.
use super::{Claim, Evidence, Payload as NoticePayload};
use crate::{Result, error::invalid};
use agent_run_config::config::BridgeDelivery;
use agent_run_domain::bridge::{BridgeIntent, BridgeOwner};
use agent_run_platform::fs::Dir;
use bridge_sdk::{Client, ClientError, TransportOutcome, bridge_protocol::*};
use std::{io::Read, os::unix::fs::MetadataExt, path::Path, time::Duration};

/// Freeze the notice and the existing binding's explicitly mapped
/// destination. Unknown transports/namespaces or invalid content refuse before socket I/O;
/// the protected credential file is read to freeze its deployment association.
pub(super) fn intent(claim: &Claim, config: &BridgeDelivery) -> Result<BridgeIntent> {
    let (kind, message) = notice_content(&claim.payload)?;
    let followup = matches!(
        &claim.payload,
        NoticePayload::Maintenance(
            agent_run_store::delivery::MaintenanceNotice::After { .. }
                | agent_run_store::delivery::MaintenanceNotice::Deferred { .. }
        )
    );
    let (owner, address) = if followup {
        let before = claim
            .maintenance_predecessor
            .as_ref()
            .ok_or_else(|| invalid("maintenance pre-notice intent is unavailable"))?;
        let address = NativeAddress::parse(&before.address)
            .map_err(|_| invalid("invalid maintenance pre-notice destination"))?
            .canonical();
        let fingerprint = publication_intent_fingerprint(
            &params(before).map_err(|_| invalid("invalid maintenance pre-notice intent"))?,
        )
        .map_err(|_| invalid("invalid maintenance pre-notice fingerprint"))?;
        if address != before.address
            || before.kind != "agent-run.maintenance"
            || before.owner.producer != "agent-run"
            || fingerprint != before.intent_hash
        {
            return Err(invalid("maintenance pre-notice intent does not verify"));
        }
        (before.owner.clone(), address)
    } else {
        if claim.maintenance_predecessor.is_some() {
            return Err(invalid("unexpected maintenance pre-notice intent"));
        }
        let (host, namespace) = match claim.transport.as_str() {
            "codex_queue" => ("codex", config.codex_store.as_deref()),
            "claude_uds" => ("claude-code", config.claude_store.as_deref()),
            _ => return Err(invalid("unsupported Bridge destination transport")),
        };
        let namespace =
            namespace.ok_or_else(|| invalid("Bridge destination namespace is not configured"))?;
        let encode = |value: &str| value.replace('%', "%25").replace('/', "%2F");
        let address = NativeAddress::parse(&format!(
            "{}/{host}/{}/{}",
            encode(&config.host_id),
            encode(namespace),
            encode(&claim.session)
        ))
        .map_err(|_| invalid("invalid Bridge destination"))?
        .canonical();
        let (_, credential_sha256) = client(config)
            .map_err(|_| invalid("protected Bridge producer credential unavailable"))?;
        (
            BridgeOwner {
                registration_socket: config.registration_socket.clone(),
                core_socket: config.core_socket.clone(),
                token_file: config.token_file.clone(),
                credential_sha256,
                producer: "agent-run".into(),
            },
            address,
        )
    };
    let identity = agent_run_platform::fs::sha256(claim.delivery_id.as_bytes());
    let mut intent = BridgeIntent {
        owner,
        request_id: format!("ar-publish-{identity}"),
        event_id: format!("ar-notice-{identity}"),
        address,
        kind: kind.into(),
        message,
        predecessor: claim
            .maintenance_predecessor
            .as_ref()
            .map(|before| before.event_id.clone()),
        intent_hash: String::new(),
    };
    intent.intent_hash = publication_intent_fingerprint(
        &params(&intent).map_err(|_| invalid("invalid Bridge publication intent"))?,
    )
    .map_err(|_| invalid("invalid Bridge publication fingerprint"))?;
    Ok(intent)
}

/// Map each validated notice to its Bridge event kind and fixed or trusted renderer.
fn notice_content(payload: &NoticePayload) -> Result<(&'static str, String)> {
    Ok(match payload {
        NoticePayload::Completion(n) => ("agent-run.completion", n.render()?),
        NoticePayload::Worker(n) => ("agent-run.worker-report", n.render()?),
        NoticePayload::Pool(n) => ("agent-run.pool-completion", n.render()?),
        NoticePayload::PoolAttention(n) => ("agent-run.pool-attention", n.render_attention()?),
        NoticePayload::Maintenance(n) => ("agent-run.maintenance", super::maintenance_message(n)?),
    })
}

/// Convert a frozen intention to the sole supported publication policy.
/// Message/KeepAll, optional predecessor ordering, no expiry or lifetime authority.
fn params(intent: &BridgeIntent) -> std::result::Result<PublishParams, ClientError> {
    let mut metadata = Metadata::default();
    if let Some(predecessor) = &intent.predecessor {
        metadata.insert("predecessor".into(), predecessor.clone());
    }
    Ok(PublishParams {
        destination: Destination::Address {
            address: intent.address.clone(),
        },
        event_id: intent.event_id.clone(),
        kind: intent.kind.clone(),
        payload: Payload::new(intent.message.clone()).map_err(ClientError::InvalidRequest)?,
        mode: Mode::Message,
        policy: Policy::KeepAll,
        metadata,
        expires_at_ms: None,
        delivery_scope_key: None,
    })
}

/// Read a bounded credential through existing no-follow descriptor helpers.
/// The opened parent and regular file must belong to this UID and be owner-only.
/// No credential, filesystem path or low-level error is exposed in diagnostics.
fn client(config: &BridgeDelivery) -> std::result::Result<(Client, String), ClientError> {
    client_with_token(config, &config.token_file)
}

/// Opens the explicit protected credential for one caller-selected Bridge role.
/// Reads at most 16 KiB; never falls back to another file or expands a grant.
pub(crate) fn client_with_token(
    config: &BridgeDelivery,
    token_file: &Path,
) -> std::result::Result<(Client, String), ClientError> {
    let load = || -> Result<String> {
        let parent = token_file
            .parent()
            .ok_or_else(|| invalid("Bridge credential parent missing"))?;
        let directory = Dir::open_searchable(parent)?;
        let entry = directory.entry(None)?;
        let name = token_file
            .file_name()
            .ok_or_else(|| invalid("Bridge credential name missing"))?;
        let file = directory.open_file(Path::new(name))?;
        let metadata = file.metadata()?;
        // SAFETY: geteuid is a read-only process identity query.
        let uid = unsafe { libc::geteuid() };
        if entry.uid != uid
            || entry.mode & 0o077 != 0
            || metadata.uid() != uid
            || metadata.mode() & 0o077 != 0
            || metadata.len() > 16384
        {
            return Err(invalid("Bridge credential protection is invalid"));
        }
        let mut bytes = Vec::new();
        file.take(16385).read_to_end(&mut bytes)?;
        if bytes.len() > 16384 {
            return Err(invalid("Bridge credential exceeds bound"));
        }
        let text =
            String::from_utf8(bytes).map_err(|_| invalid("Bridge credential must be UTF-8"))?;
        Ok(text.trim_end_matches(['\r', '\n']).to_owned())
    };
    let credential = load()
        .map_err(|_| ClientError::InvalidConfig("protected producer credential unavailable"))?;
    let fingerprint = agent_run_platform::fs::sha256(credential.as_bytes());
    let client = Client::new(bridge_sdk::Config::new(
        &config.registration_socket,
        &config.core_socket,
        Secret::new(credential),
        Duration::from_secs(5),
    )?)?;
    Ok((client, fingerprint))
}

/// Validate full historical custody against the frozen normalized publication,
/// original expected producer and address form; status-selector validation alone is insufficient.
fn custody(
    intent: &BridgeIntent,
    receipt: EventReceipt,
) -> std::result::Result<EventReceipt, ClientError> {
    if receipt.producer != intent.owner.producer
        || receipt.event_id != intent.event_id
        || receipt.intent_hash != intent.intent_hash
        || receipt.destination.address != intent.address
        || receipt.destination.route_key.is_some()
        || receipt.destination.caller_scope.is_some()
        || receipt.destination.destination_scope.is_some()
    {
        return Err(ClientError::Transport {
            outcome: TransportOutcome::OutcomeUnknown,
            reconciliation: Some(ReconciliationKey::Event {
                event_id: intent.event_id.clone(),
            }),
            reason: "custody does not match frozen publication owner and fingerprint",
        });
    }
    Ok(receipt)
}

/// Fresh publication uses only its frozen deployment/credential association.
/// Unknown custody or changed key performs read-only same-owner status reconciliation;
/// absence is held, never republished. Positive exact history survives grant tightening
/// and verified same-publisher key rotation; changing endpoints/files never re-homes it.
fn publish(
    config: &BridgeDelivery,
    intent: &BridgeIntent,
    was_unknown: bool,
) -> std::result::Result<EventReceipt, ClientError> {
    if config.registration_socket != intent.owner.registration_socket
        || config.core_socket != intent.owner.core_socket
        || config.token_file != intent.owner.token_file
    {
        return Err(ClientError::InvalidConfig(
            "Bridge deployment owner changed",
        ));
    }
    let parameters = params(intent)?;
    if publication_intent_fingerprint(&parameters).map_err(ClientError::InvalidRequest)?
        != intent.intent_hash
    {
        return Err(ClientError::InvalidConfig(
            "frozen publication fingerprint mismatch",
        ));
    }
    let (client, fingerprint) = client(config)?;
    let rotated = fingerprint != intent.owner.credential_sha256;
    if was_unknown || rotated {
        let status = client.status(
            format!("{}-status", intent.request_id),
            StatusSelector::Event {
                event_id: intent.event_id.clone(),
            },
        )?;
        if let StatusResult::Event { event: Some(event) } = status {
            return custody(intent, event.receipt);
        }
        return Err(if was_unknown {
            ClientError::Transport {
                outcome: TransportOutcome::OutcomeUnknown,
                reconciliation: Some(ReconciliationKey::Event {
                    event_id: intent.event_id.clone(),
                }),
                reason: "original custody not visible; same-owner observation only",
            }
        } else {
            ClientError::InvalidConfig("Bridge deployment owner changed")
        });
    }
    let capabilities = client.capabilities(format!("{}-cap", intent.request_id))?;
    if capabilities.api_version != 1
        || !capabilities.grant.direct_address
        || !capabilities.operations.iter().any(|op| op == "publish")
        || !capabilities.operations.iter().any(|op| op == "status")
        || !capabilities.modes.contains(&Mode::Message)
        || !capabilities.policies.contains(&PolicyKind::KeepAll)
        || !capabilities.grant.allowed_modes.contains(&Mode::Message)
        || !capabilities
            .grant
            .allowed_policies
            .contains(&PolicyKind::KeepAll)
        || !capabilities
            .grant
            .allowed_kinds
            .iter()
            .any(|kind| kind == "*" || kind == &intent.kind)
        || intent.message.len() as u64 > capabilities.limits.payload_bytes
    {
        return Err(ClientError::InvalidConfig(
            "producer grant does not support frozen delivery",
        ));
    }
    custody(
        intent,
        client.publish(&intent.request_id, parameters)?.receipt,
    )
}

/// Classify only safe effect facts, retaining a previously unknown custody outcome.
/// No raw SDK error/response/payload enters immutable delivery evidence.
fn observation(
    result: std::result::Result<EventReceipt, ClientError>,
    was_unknown: bool,
) -> (Evidence, Option<serde_json::Value>) {
    let owner_changed = matches!(
        &result,
        Err(ClientError::InvalidConfig(
            "Bridge deployment owner changed"
        ))
    );
    let receipt = result
        .as_ref()
        .ok()
        .and_then(|receipt| serde_json::to_value(receipt).ok());
    let (classifier, accepted, unknown) = match result {
        Ok(_) => ("bridge_custody_accepted", true, false),
        Err(error) => {
            let unknown = was_unknown
                || matches!(
                    &error,
                    ClientError::Transport {
                        outcome: TransportOutcome::OutcomeUnknown,
                        ..
                    }
                )
                || matches!(&error, ClientError::Service(e) if matches!(e.effect, Some(MutationEffect::Unknown | MutationEffect::Committed | MutationEffect::CommittedReplay)));
            let classifier = if unknown {
                "bridge_outcome_unknown"
            } else {
                match error {
                    ClientError::InvalidConfig("Bridge deployment owner changed") => {
                        "bridge_owner_changed"
                    }
                    ClientError::InvalidConfig(_) => "bridge_unconfigured",
                    ClientError::InvalidRequest(_) => "bridge_invalid",
                    ClientError::Service(e) => match e.class {
                        ErrorClass::Auth | ErrorClass::Forbidden => "bridge_forbidden",
                        ErrorClass::Unsupported => "bridge_unsupported",
                        ErrorClass::Invalid | ErrorClass::Conflict => "bridge_refused",
                        _ => "bridge_unavailable",
                    },
                    ClientError::Transport { .. } => "bridge_unavailable",
                }
            };
            (classifier, false, unknown)
        }
    };
    let mut evidence = Evidence::new(classifier, accepted, unknown);
    if owner_changed {
        evidence.error_class = Some("owner_changed".into());
    }
    (evidence, receipt)
}

/// Execute bounded SDK I/O off the runtime thread. Cancellation/crash can only
/// recover the same persisted intent; a failed join after possible I/O stays unknown.
pub(super) async fn send(
    config: Option<BridgeDelivery>,
    intent: BridgeIntent,
    was_unknown: bool,
) -> (Evidence, Option<serde_json::Value>) {
    let Some(config) = config else {
        return observation(
            Err(ClientError::InvalidConfig(
                "Bridge delivery remains owned but unconfigured",
            )),
            was_unknown,
        );
    };
    match tokio::task::spawn_blocking(move || publish(&config, &intent, was_unknown)).await {
        Ok(result) => observation(result, was_unknown),
        Err(_) => observation(
            Err(ClientError::Transport {
                outcome: TransportOutcome::OutcomeUnknown,
                reconciliation: None,
                reason: "Bridge sender task ended",
            }),
            true,
        ),
    }
}

#[cfg(test)]
mod tests {
    //! Synthetic namespace/rendering and protected-file controls; no native delivery claim.
    use super::*;
    use agent_run_domain::{
        domain::Status,
        pool::PoolNotice,
        worker::{WorkerMessageKind, WorkerNotice},
    };
    use std::os::unix::fs::PermissionsExt;

    /// Maintenance rendering and routing are pure: no credential or socket is
    /// consulted, and held work always receives an explicit attention prompt.
    #[test]
    fn maintenance_notice_uses_bridge_kind_and_honest_fixed_text() {
        use agent_run_store::delivery::MaintenanceNotice;
        let stages = [
            (
                MaintenanceNotice::Pre {
                    maintenance_epoch: 12,
                },
                "New task admissions were blocked for this maintenance epoch.",
            ),
            (
                MaintenanceNotice::Deferred {
                    maintenance_epoch: 12,
                },
                "Agent Run deferred this maintenance and reopened task admissions",
            ),
            (
                MaintenanceNotice::After {
                    maintenance_epoch: 12,
                    resumed: 4,
                    completed: 3,
                    failed: 1,
                    cancelled: 2,
                    held: 1,
                },
                "Executions: resumed 4; completed 3; failed 1; cancelled 2; held 1.",
            ),
        ];
        for (notice, expected) in stages {
            let (kind, text) = notice_content(&NoticePayload::Maintenance(notice)).unwrap();
            assert_eq!(kind, "agent-run.maintenance");
            assert!(text.contains(expected));
            assert!(text.contains("Maintenance epoch: 12"));
            assert!(!text.contains("delivered"));
        }
        let (_, pre) = notice_content(&NoticePayload::Maintenance(MaintenanceNotice::Pre {
            maintenance_epoch: 1,
        }))
        .unwrap();
        assert!(pre.contains("started preparing agents to pause"));
        assert!(pre.contains("No action is required from the orchestrator"));
        let (_, deferred) =
            notice_content(&NoticePayload::Maintenance(MaintenanceNotice::Deferred {
                maintenance_epoch: 1,
            }))
            .unwrap();
        assert!(deferred.contains("The restart was not completed"));
        assert!(deferred.contains("Individual agent notices report failures"));
        let (_, held) = notice_content(&NoticePayload::Maintenance(MaintenanceNotice::After {
            maintenance_epoch: 1,
            resumed: 0,
            completed: 0,
            failed: 0,
            cancelled: 0,
            held: 1,
        }))
        .unwrap();
        assert!(held.contains("Attention required"));
        assert!(!held.contains("No action is required"));
        let (_, clear) = notice_content(&NoticePayload::Maintenance(MaintenanceNotice::After {
            maintenance_epoch: 1,
            resumed: 0,
            completed: 0,
            failed: 0,
            cancelled: 0,
            held: 0,
        }))
        .unwrap();
        assert!(!clear.contains("Attention required"));
        assert!(clear.contains("No action is required"));
        let (_, failed) = notice_content(&NoticePayload::Maintenance(MaintenanceNotice::After {
            maintenance_epoch: 1,
            resumed: 0,
            completed: 0,
            failed: 1,
            cancelled: 0,
            held: 0,
        }))
        .unwrap();
        assert!(failed.contains("Attention required"));
        assert!(!failed.contains("No action is required"));
        assert!(
            notice_content(&NoticePayload::Maintenance(MaintenanceNotice::Pre {
                maintenance_epoch: 0,
            }))
            .is_err()
        );
    }

    /// A follow-up retains the accepted pre-notice's owner and canonical
    /// destination even if mutable configuration changes between messages.
    #[test]
    fn maintenance_followup_keeps_original_owner_and_destination() {
        use agent_run_store::delivery::MaintenanceNotice;
        let home = tempfile::tempdir().unwrap();
        let directory = home.path().canonicalize().unwrap();
        agent_run_platform::fs::private_dir(&directory).unwrap();
        let token = directory.join("token");
        std::fs::write(&token, "synthetic-private-producer-credential").unwrap();
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
        let config = BridgeDelivery {
            registration_socket: directory.join("a.sock"),
            core_socket: directory.join("c.sock"),
            token_file: token,
            observation_token_file: None,
            host_id: "host-one".into(),
            codex_store: Some("store-one".into()),
            claude_store: None,
            opencode_store: None,
            opencode_plugin: None,
        };
        let mut claim = Claim {
            delivery_id: "ntf_before".into(),
            lease_owner: "fixture".into(),
            attempt: 1,
            transport: "codex_queue".into(),
            session: "session-one".into(),
            payload: NoticePayload::Maintenance(MaintenanceNotice::Pre {
                maintenance_epoch: 1,
            }),
            policy: Default::default(),
            bridge_intent: None,
            maintenance_predecessor: None,
            bridge_unknown: false,
        };
        let before = intent(&claim, &config).unwrap();
        claim.delivery_id = "ntf_after".into();
        claim.payload = NoticePayload::Maintenance(MaintenanceNotice::After {
            maintenance_epoch: 1,
            resumed: 1,
            completed: 0,
            failed: 0,
            cancelled: 0,
            held: 0,
        });
        claim.maintenance_predecessor = Some(before.clone());
        let mut changed = config.clone();
        changed.host_id = "host-two".into();
        changed.codex_store = Some("store-two".into());
        changed.registration_socket = directory.join("different.sock");
        let after = intent(&claim, &changed).unwrap();
        assert_eq!(after.address, before.address);
        assert!(after.owner == before.owner);
        assert_eq!(after.predecessor.as_deref(), Some(before.event_id.as_str()));
        assert_eq!(
            params(&after).unwrap().metadata.get("predecessor"),
            Some(&before.event_id)
        );
        assert!(
            !params(&before)
                .unwrap()
                .metadata
                .contains_key("predecessor")
        );
        assert_ne!(after.event_id, before.event_id);
        assert_ne!(after.intent_hash, before.intent_hash);
        assert!(matches!(
            publish(&changed, &after, false),
            Err(ClientError::InvalidConfig(
                "Bridge deployment owner changed"
            ))
        ));
        claim.maintenance_predecessor.as_mut().unwrap().intent_hash = "0".repeat(64);
        assert!(intent(&claim, &changed).is_err());
        claim.maintenance_predecessor = None;
        assert!(intent(&claim, &changed).is_err());
    }

    /// All external notice categories retain their distinct rendering, stable IDs
    /// and explicitly configured lossless native namespaces without socket I/O.
    #[test]
    fn four_notice_kinds_keep_bound_destination_and_content() {
        let agent: agent_run_domain::domain::AgentId =
            "ag-20260825-120000-0123456789".parse().unwrap();
        let pool = PoolNotice {
            notification_id: "ntf_pool".into(),
            pool_id: "pool-20261003-120000-0123456789".parse().unwrap(),
            message: "reviewed".into(),
        };
        let home = tempfile::tempdir().unwrap();
        let directory = home.path().canonicalize().unwrap();
        agent_run_platform::fs::private_dir(&directory).unwrap();
        let token = directory.join("token");
        std::fs::write(&token, "synthetic-private-producer-credential").unwrap();
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
        let config = BridgeDelivery {
            registration_socket: directory.join("a.sock"),
            core_socket: directory.join("c.sock"),
            token_file: token,
            observation_token_file: None,
            host_id: "host/part".into(),
            codex_store: Some("store%name".into()),
            claude_store: Some("claude-main".into()),
            opencode_store: None,
            opencode_plugin: None,
        };
        for (payload, kind, marker) in [
            (
                NoticePayload::Completion(super::super::Notice::legacy(
                    "ntf_done",
                    agent.clone(),
                    Status::Succeeded,
                )),
                "agent-run.completion",
                "agent-run/completion",
            ),
            (
                NoticePayload::Worker(WorkerNotice {
                    notification_id: "ntf_report".into(),
                    agent_id: agent.clone(),
                    run_id: agent,
                    kind: WorkerMessageKind::Risk,
                    message: "review needed".into(),
                }),
                "agent-run.worker-report",
                "agent-run/worker-message",
            ),
            (
                NoticePayload::Pool(pool.clone()),
                "agent-run.pool-completion",
                "agent-run/pool-completion",
            ),
            (
                NoticePayload::PoolAttention(pool),
                "agent-run.pool-attention",
                "agent-run/pool-attention",
            ),
        ] {
            let mut claim = Claim {
                delivery_id: "ntf_same".into(),
                lease_owner: "owned".into(),
                attempt: 1,
                transport: "codex_queue".into(),
                session: "session/part".into(),
                payload,
                policy: Default::default(),
                bridge_intent: None,
                maintenance_predecessor: None,
                bridge_unknown: false,
            };
            let frozen = intent(&claim, &config).unwrap();
            assert_eq!(frozen.kind, kind);
            assert!(frozen.message.contains(marker));
            assert_eq!(
                frozen.address,
                "host%2Fpart/codex/store%25name/session%2Fpart"
            );
            claim.transport = "claude_uds".into();
            let claude = intent(&claim, &config).unwrap();
            assert_eq!(
                claude.address,
                "host%2Fpart/claude-code/claude-main/session%2Fpart"
            );
            assert_eq!(frozen.event_id, claude.event_id);
            assert_eq!(frozen.request_id, claude.request_id);
        }
    }

    /// Credential loading accepts only an owned protected regular file, rejects
    /// exposed files/symlinks, and never includes file contents in error formatting.
    #[test]
    fn credential_reader_refuses_unprotected_and_symlinked_files() {
        let home = tempfile::tempdir().unwrap();
        let directory = home.path().canonicalize().unwrap();
        agent_run_platform::fs::private_dir(&directory).unwrap();
        let token = directory.join("token");
        std::fs::write(&token, "synthetic-private-producer-credential").unwrap();
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
        let config = BridgeDelivery {
            registration_socket: directory.join("a.sock"),
            core_socket: directory.join("c.sock"),
            token_file: token.clone(),
            observation_token_file: None,
            host_id: "local".into(),
            codex_store: Some("codex-main".into()),
            claude_store: None,
            opencode_store: None,
            opencode_plugin: None,
        };
        assert!(client(&config).is_ok());
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o750)).unwrap();
        assert!(client(&config).is_err());
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o644)).unwrap();
        let error = client(&config).unwrap_err();
        assert!(!format!("{error:?}").contains("synthetic-private-producer-credential"));
        std::fs::rename(&token, directory.join("real-token")).unwrap();
        std::os::unix::fs::symlink("real-token", &token).unwrap();
        assert!(client(&config).is_err());
        std::fs::remove_file(&token).unwrap();
        std::fs::write(&token, vec![b'x'; 16_385]).unwrap();
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(client(&config).is_err());
        std::fs::write(&token, "synthetic-private-producer-credential").unwrap();
        assert!(client(&config).is_ok());

        let link = directory.parent().unwrap().join(format!(
            "{}-link",
            directory.file_name().unwrap().to_string_lossy()
        ));
        std::os::unix::fs::symlink(&directory, &link).unwrap();
        let mut linked = config;
        linked.token_file = link.join("token");
        assert!(client(&linked).is_err());
        std::fs::remove_file(link).unwrap();
    }

    /// Observer references are explicitly separate and absent from historical
    /// serialization; no delivery credential is automatically granted reads.
    #[test]
    fn observation_credential_is_additive_and_separate() {
        let original = serde_json::json!({"registration_socket":"/private/a.sock","core_socket":"/private/c.sock","token_file":"/private/delivery","host_id":"local","codex_store":"main","claude_store":null});
        let mut config: BridgeDelivery = serde_json::from_value(original.clone()).unwrap();
        config.validate().unwrap();
        assert_eq!(serde_json::to_value(&config).unwrap(), original);
        config.observation_token_file = Some(config.token_file.clone());
        assert!(config.validate().is_err());
        config.observation_token_file = Some("relative".into());
        assert!(config.validate().is_err());
        config.observation_token_file = Some("/private/observer".into());
        config.validate().unwrap();
    }
}
