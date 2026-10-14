//! Delivery-only Bridge SDK adapter; no native caller or execution authority.
use super::{Claim, Evidence, Payload as NoticePayload};
use crate::{Result, error::invalid};
use agent_run_config::config::BridgeDelivery;
use agent_run_domain::bridge::{BridgeIntent, BridgeOwner};
use agent_run_platform::fs::Dir;
use bridge_sdk::{Client, ClientError, TransportOutcome, bridge_protocol::*};
use std::{io::Read, os::unix::fs::MetadataExt, path::Path, time::Duration};

/// Freeze the four notice forms and the existing binding's explicitly mapped
/// destination. Unknown transports/namespaces or invalid content refuse before socket I/O;
/// the protected credential file is read to freeze its deployment association.
pub(super) fn intent(claim: &Claim, config: &BridgeDelivery) -> Result<BridgeIntent> {
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
    let (kind, message) = match &claim.payload {
        NoticePayload::Completion(n) => ("agent-run.completion", n.render()?),
        NoticePayload::Worker(n) => ("agent-run.worker-report", n.render()?),
        NoticePayload::Pool(n) => ("agent-run.pool-completion", n.render()?),
        NoticePayload::PoolAttention(n) => ("agent-run.pool-attention", n.render_attention()?),
    };
    let identity = agent_run_platform::fs::sha256(claim.delivery_id.as_bytes());
    let (_, credential_sha256) =
        client(config).map_err(|_| invalid("protected Bridge producer credential unavailable"))?;
    let mut intent = BridgeIntent {
        owner: BridgeOwner {
            registration_socket: config.registration_socket.clone(),
            core_socket: config.core_socket.clone(),
            token_file: config.token_file.clone(),
            credential_sha256,
            producer: "agent-run".into(),
        },
        request_id: format!("ar-publish-{identity}"),
        event_id: format!("ar-notice-{identity}"),
        address,
        kind: kind.into(),
        message,
        intent_hash: String::new(),
    };
    intent.intent_hash = publication_intent_fingerprint(
        &params(&intent).map_err(|_| invalid("invalid Bridge publication intent"))?,
    )
    .map_err(|_| invalid("invalid Bridge publication fingerprint"))?;
    Ok(intent)
}

/// Convert a frozen intention to the sole supported publication policy.
/// Message/KeepAll, no expiry, metadata, route registration or lifetime authority.
fn params(intent: &BridgeIntent) -> std::result::Result<PublishParams, ClientError> {
    Ok(PublishParams {
        destination: Destination::Address {
            address: intent.address.clone(),
        },
        event_id: intent.event_id.clone(),
        kind: intent.kind.clone(),
        payload: Payload::new(intent.message.clone()).map_err(ClientError::InvalidRequest)?,
        mode: Mode::Message,
        policy: Policy::KeepAll,
        metadata: Metadata::default(),
        expires_at_ms: None,
        delivery_scope_key: None,
    })
}

/// Read a bounded credential through existing no-follow descriptor helpers.
/// The opened parent and regular file must belong to this UID and be owner-only.
/// No credential, filesystem path or low-level error is exposed in diagnostics.
fn client(config: &BridgeDelivery) -> std::result::Result<(Client, String), ClientError> {
    let load = || -> Result<String> {
        let parent = config
            .token_file
            .parent()
            .ok_or_else(|| invalid("Bridge credential parent missing"))?;
        let relative = parent
            .strip_prefix("/")
            .map_err(|_| invalid("Bridge credential path must be absolute"))?;
        let directory = Dir::open(Path::new("/"))?.subdir(relative)?;
        let entry = directory.entry(None)?;
        let name = config
            .token_file
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
        agent_run_platform::fs::private_dir(home.path()).unwrap();
        let token = home.path().join("token");
        std::fs::write(&token, "synthetic-private-producer-credential").unwrap();
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
        let config = BridgeDelivery {
            registration_socket: home.path().join("a.sock"),
            core_socket: home.path().join("c.sock"),
            token_file: token,
            host_id: "host/part".into(),
            codex_store: Some("store%name".into()),
            claude_store: Some("claude-main".into()),
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
        agent_run_platform::fs::private_dir(home.path()).unwrap();
        let token = home.path().join("token");
        std::fs::write(&token, "synthetic-private-producer-credential").unwrap();
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
        let config = BridgeDelivery {
            registration_socket: home.path().join("a.sock"),
            core_socket: home.path().join("c.sock"),
            token_file: token.clone(),
            host_id: "local".into(),
            codex_store: Some("codex-main".into()),
            claude_store: None,
        };
        assert!(client(&config).is_ok());
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o644)).unwrap();
        let error = client(&config).unwrap_err();
        assert!(!format!("{error:?}").contains("synthetic-private-producer-credential"));
        std::fs::rename(&token, home.path().join("real-token")).unwrap();
        std::os::unix::fs::symlink("real-token", &token).unwrap();
        assert!(client(&config).is_err());
    }
}
