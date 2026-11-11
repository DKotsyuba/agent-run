//! Closed private R2 NDJSON operations. Not bound to the public API, MCP or
//! default broker endpoints; callers must configure and authenticate peers.
#![allow(dead_code)]
use crate::{
    Error, Result,
    attempt_binding::{
        ActorRecoveryRefusal, ActorRecoveryState, BridgeReader, EgressOutcome, PeerGrant,
        PoolReader,
    },
    attempt_transport::{PeerPin, strict, verify_peer},
    error::invalid,
    service::Service,
};
use agent_run_domain::{
    StoreId,
    attempt_binding::{
        AdmissionRequest, AttemptRef, Binding, ConsumedRef, Incarnation, IngressCurrent, Route,
    },
    domain::AgentId,
};
use agent_run_store::attempt_binding::NamespaceRequest;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    id: String,
    method: String,
    params: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Discovery {
    version: u32,
}
#[derive(Deserialize)]
#[serde(tag = "purpose", rename_all = "snake_case", deny_unknown_fields)]
enum Current {
    Ingress {
        version: u32,
        expected_store_id: StoreId,
        request_id: String,
        observation_ref: String,
        invocation_key: String,
    },
    Egress {
        version: u32,
        expected_store_id: StoreId,
        request_id: String,
        lineage_root_id: AgentId,
        bound_execution_id: AgentId,
        #[serde(default)]
        actor_recovery_version: Option<u32>,
    },
    Consensus {
        version: u32,
        expected_store_id: StoreId,
        request_id: String,
        lineage_root_id: AgentId,
        bound_execution_id: AgentId,
        expected_attempt: AttemptRef,
        expected_incarnation: Incarnation,
    },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentLookup {
    version: u32,
    expected_store_id: StoreId,
    receipt_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EffectLookup {
    version: u32,
    expected_store_id: StoreId,
    producer_scope: String,
    effect_id: String,
    effect_digest: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Admit {
    version: u32,
    expected_store_id: StoreId,
    request_id: String,
    intent_id: String,
    security_digest: String,
    binding: Binding,
    consumed_ref: ConsumedRef,
    policy: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Lookup {
    version: u32,
    expected_store_id: StoreId,
    intent_id: String,
    security_digest: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Abort {
    version: u32,
    expected_store_id: StoreId,
    request_id: String,
    intent_id: String,
    security_digest: String,
    pool_route: Route,
    pool_outcome_digest: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settle {
    version: u32,
    expected_store_id: StoreId,
    admission_id: String,
    intent_id: String,
    security_digest: String,
    pool_outcome_digest: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Retire {
    version: u32,
    expected_store_id: StoreId,
    request_id: String,
    bridge_route: Route,
    producer: String,
    issuer_epoch: String,
    retirement_key: String,
    pool_route: Route,
    pool_seal_request_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnrollmentCurrent {
    version: u32,
    expected_store_id: StoreId,
    pool_store_id: String,
    request_id: String,
    fingerprint: String,
    member_id: String,
    current_request_id: String,
}

fn parse<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
    if !value.is_object() {
        return Err(invalid("attempt request invalid"));
    }
    serde_json::from_value(value).map_err(|_| invalid("attempt request invalid"))
}
fn require_object(value: &Value, path: &str) -> Result<()> {
    if value.pointer(path).is_some_and(Value::is_object) {
        Ok(())
    } else {
        Err(invalid("attempt request invalid"))
    }
}
fn version(n: u32) -> Result<()> {
    if n == 1 {
        Ok(())
    } else {
        Err(Error::Unsupported("attempt version unsupported".into()))
    }
}
fn closed_error(error: &Error, _method: &str, store: &str) -> Value {
    if let Error::Broker {
        broker_error_code: Some(code),
        broker_error_data: Some(data),
        ..
    } = error
    {
        let recovery = data.get("recovery").and_then(Value::as_str);
        if matches!(
            code.as_str(),
            "unsupported_version"
                | "capability_missing"
                | "transport_unsupported"
                | "role_unqualified"
                | "peer_not_authorized"
                | "audience_mismatch"
                | "binding_unavailable"
                | "attempt_not_ready"
                | "attempt_mismatch"
                | "actor_mismatch"
                | "route_mismatch"
                | "assertion_stale"
                | "assertion_historical"
                | "invocation_spent"
                | "intent_conflict"
                | "request_in_progress"
                | "capacity_exhausted"
                | "aborted"
                | "recovery_required"
                | "outcome_unknown"
        ) && matches!(
            recovery,
            Some("none" | "same_key_lookup" | "fresh_invocation" | "operator")
        ) {
            return json!({"version":1,"store_id":store,"code":code,"recovery":recovery});
        }
    }
    let (code, recovery) = match error {
        Error::Persistence { source, .. }
            if matches!(
                source.as_ref(),
                Error::WriterFailure(agent_run_domain::storage::WriterFailure::Unknown(_))
            ) =>
        {
            ("outcome_unknown", "same_key_lookup")
        }
        Error::WriterFailure(agent_run_domain::storage::WriterFailure::Unknown(_)) => {
            ("outcome_unknown", "same_key_lookup")
        }
        Error::BatchOutcomeUnknown { .. } => ("outcome_unknown", "same_key_lookup"),
        Error::PreconditionFailed { .. } => ("precondition_failed", "none"),
        Error::Validation(message) if message == "attempt peer unavailable" => {
            ("outcome_unknown", "same_key_lookup")
        }
        Error::StoreIdentityMismatch => ("recovery_required", "operator"),
        Error::Unsupported(message) if message == "attempt version unsupported" => {
            ("unsupported_version", "none")
        }
        Error::Unsupported(_) => ("capability_missing", "none"),
        Error::Validation(message) if message == "attempt not ready" => {
            ("attempt_not_ready", "same_key_lookup")
        }
        Error::Validation(message)
            if matches!(
                message.as_str(),
                "attempt mismatch" | "attempt peer executable changed"
            ) =>
        {
            ("attempt_mismatch", "none")
        }
        Error::Validation(message) if message == "attempt peer verification failed" => {
            ("peer_not_authorized", "none")
        }
        Error::Validation(message) if message == "actor mismatch" => ("actor_mismatch", "none"),
        Error::Validation(message) if message == "invalid attempt actor" => {
            ("actor_mismatch", "none")
        }
        Error::Validation(message) if message == "route mismatch" => ("route_mismatch", "none"),
        Error::Validation(message) if message == "binding stale" => {
            ("binding_stale", "fresh_invocation")
        }
        Error::Validation(message) if message == "assertion historical" => {
            ("assertion_historical", "fresh_invocation")
        }
        Error::Validation(message) if message == "namespace recovery in progress" => {
            ("request_in_progress", "same_key_lookup")
        }
        Error::Validation(message) if message == "actor recovery in progress" => {
            ("request_in_progress", "same_key_lookup")
        }
        Error::Validation(message) if message == "enrollment in progress" => {
            ("request_in_progress", "same_key_lookup")
        }
        Error::Validation(message) if message == "enrollment aborted" => {
            ("enrollment_aborted", "none")
        }
        Error::Validation(message) if message == "enrollment role mismatch" => {
            ("role_mismatch", "none")
        }
        Error::Validation(message) if message == "enrollment role unqualified" => {
            ("role_unqualified", "none")
        }
        Error::Validation(message) if message == "enrollment transport changed" => {
            ("recovery_required", "operator")
        }
        Error::Validation(message) if message == "namespace recovery incomplete" => {
            ("recovery_required", "operator")
        }
        Error::Validation(message) if message == "attempt peer configuration unavailable" => {
            ("recovery_required", "operator")
        }
        Error::Validation(message) if message == "attempt capacity exhausted" => {
            ("capacity_exhausted", "operator")
        }
        Error::Capacity => ("capacity_exhausted", "operator"),
        Error::Conflict => ("intent_conflict", "none"),
        Error::Validation(_) => ("binding_unavailable", "operator"),
        _ => ("recovery_required", "operator"),
    };
    json!({"version":1,"store_id":store,"code":code,"recovery":recovery})
}

#[derive(Debug)]
pub(crate) enum DispatchFailure {
    Domain(Box<Error>),
    ActorRecovery(Box<ActorRecoveryRefusal>),
}
impl From<Error> for DispatchFailure {
    fn from(error: Error) -> Self {
        Self::Domain(Box::new(error))
    }
}

pub(crate) fn dispatch_error(error: &DispatchFailure, method: &str, store: &str) -> Value {
    match error {
        DispatchFailure::Domain(error) => closed_error(error, method, store),
        DispatchFailure::ActorRecovery(refusal) => {
            let recovery = match refusal.state {
                ActorRecoveryState::AwaitingRootHook => "same_key_lookup",
                ActorRecoveryState::RelaunchRequired | ActorRecoveryState::Unavailable => {
                    "operator"
                }
                ActorRecoveryState::Current => {
                    return closed_error(&invalid("actor recovery in progress"), method, store);
                }
            };
            json!({"version":1,"store_id":store,"code":"binding_unavailable","recovery":recovery,
                "request_id":refusal.request_id,"actor_recovery_version":1,"actor_recovery":refusal.state,
                "incarnation":refusal.incarnation,"launch_id":refusal.launch_id})
        }
    }
}

/// Dispatch one already authenticated request. Each caller is tied to the
/// configured grant; request data never chooses a socket, executable or token.
pub(crate) async fn dispatch(
    service: &Service,
    grant: &PeerGrant,
    bridge: &dyn BridgeReader,
    pool: &dyn PoolReader,
    method: &str,
    params: Value,
) -> std::result::Result<Value, DispatchFailure> {
    match method {
        "_attempt_capabilities_v1" => {
            let req: Discovery = parse(params)?;
            version(req.version)?;
            let store = service
                .read(|reader| agent_run_store::store_identity::read(&reader.conn))
                .await?;
            let capabilities = if service.attempt_enabled(grant).is_ok() {
                vec![
                    "attempt_current_ingress_v1",
                    "attempt_current_egress_v1",
                    "attempt_egress_actor_recovery_v1",
                    "attempt_consensus_v1",
                    "attempt_admission_v1",
                    "attempt_namespace_retirement_v1",
                    "managed_enrollment_v1",
                ]
            } else {
                Vec::new()
            };
            Ok(
                json!({"version":1,"store_id":store,"capabilities":capabilities,"policy":"admission_snapshot"}),
            )
        }
        "_attempt_current_v1" => {
            let request: Current = parse(params.clone())?;
            match request {
                Current::Ingress {
                    version: n,
                    expected_store_id,
                    request_id,
                    observation_ref,
                    invocation_key,
                } => {
                    version(n)?;
                    reserved(&request_id)?;
                    let req = IngressCurrent {
                        expected_store_id: expected_store_id.clone(),
                        request_id,
                        bridge_route: grant.bridge_route.clone(),
                        pool_route: grant.pool_route.clone(),
                        run_route: grant.run_route.clone(),
                        producer: grant.producer.clone(),
                        bridge_context_ref: observation_ref,
                        invocation_key,
                    };
                    let receipt = service
                        .attempt_current_ingress_v1(grant, bridge, req)
                        .await?;
                    Ok(
                        json!({"version":1,"store_id":expected_store_id,"state":"current","receipt":receipt}),
                    )
                }
                Current::Egress {
                    version: n,
                    expected_store_id,
                    request_id,
                    lineage_root_id,
                    bound_execution_id,
                    actor_recovery_version,
                } => {
                    version(n)?;
                    reserved(&request_id)?;
                    if params
                        .get("actor_recovery_version")
                        .is_some_and(Value::is_null)
                    {
                        return Err(invalid("attempt request invalid").into());
                    }
                    if actor_recovery_version.is_some_and(|version| version != 1) {
                        return Err(Error::Unsupported("attempt version unsupported".into()).into());
                    }
                    let req = agent_run_domain::attempt_binding::EgressCurrent {
                        expected_store_id: expected_store_id.clone(),
                        request_id,
                        lineage_root_id,
                        bound_execution_id,
                        bridge_route: grant.bridge_route.clone(),
                        pool_route: grant.pool_route.clone(),
                        run_route: grant.run_route.clone(),
                        producer: grant.producer.clone(),
                        host_id: grant.host_id.clone(),
                    };
                    match service
                        .attempt_current_egress_with_recovery(
                            grant,
                            bridge,
                            req,
                            actor_recovery_version == Some(1),
                        )
                        .await?
                    {
                        EgressOutcome::Current(receipt) => Ok(
                            json!({"version":1,"store_id":expected_store_id,"state":"current","receipt":receipt}),
                        ),
                        EgressOutcome::ActorRecovery(refusal) => {
                            Err(DispatchFailure::ActorRecovery(Box::new(refusal)))
                        }
                    }
                }
                Current::Consensus {
                    version: n,
                    expected_store_id,
                    request_id,
                    lineage_root_id,
                    bound_execution_id,
                    expected_attempt,
                    expected_incarnation,
                } => {
                    version(n)?;
                    wire_id(&request_id)?;
                    for path in ["/expected_attempt", "/expected_incarnation"] {
                        require_object(&params, path)?;
                    }
                    let view = service
                        .attempt_consensus_v1(
                            grant,
                            expected_store_id.clone(),
                            lineage_root_id,
                            bound_execution_id,
                            expected_attempt,
                            expected_incarnation,
                        )
                        .await?;
                    Ok(
                        json!({"version":1,"store_id":expected_store_id,"state":view.state,"last_attempt":view.last_attempt,"last_incarnation":view.last_incarnation,"successor_proof":view.successor_proof}),
                    )
                }
            }
        }
        "_managed_start_batch_v1" => {
            let (request, fingerprint) = agent_run_domain::enrollment::Start::parse(&params)?;
            let receipt = service
                .start_managed_enrollment(grant, request, params, fingerprint)
                .await?;
            Ok(serde_json::to_value(receipt).map_err(Error::from)?)
        }
        "_managed_start_lookup_v1" | "_managed_start_abort_v1" => {
            let key: agent_run_domain::enrollment::Key = parse(params)?;
            key.validate(&grant.pool_route)?;
            let stored = if method == "_managed_start_abort_v1" {
                Some(
                    service
                        .lane("admission")?
                        .local(
                            agent_run_store::ops::AbortManagedEnrollment {
                                key: key.clone(),
                                pool_route: grant.pool_route.clone(),
                            },
                            crate::broker_writer::BROKER_BUDGET,
                        )
                        .await?,
                )
            } else {
                let lookup_key = key.clone();
                let route = grant.pool_route.clone();
                service
                    .read(move |store| store.managed_enrollment_lookup(&lookup_key, &route))
                    .await?
            };
            if stored.as_ref().is_some_and(|row| row.state == "prepared") {
                return Err(invalid("enrollment in progress").into());
            }
            let state = stored
                .as_ref()
                .map_or_else(|| "absent".to_owned(), |row| row.state.clone());
            let receipt = stored.and_then(|row| row.receipt);
            let jobs_state = if let Some(receipt) = &receipt {
                let source = receipt.clone();
                service
                    .read(move |store| store.managed_enrollment_jobs_state(&source))
                    .await?
            } else {
                Vec::new()
            };
            Ok(
                json!({"version":1,"store_id":key.expected_store_id,"request_id":key.request_id,"fingerprint":key.fingerprint,"state":state,"receipt":receipt,"jobs_state":jobs_state}),
            )
        }
        "_managed_enrollment_current_v1" => {
            let req: EnrollmentCurrent = parse(params)?;
            let key = agent_run_domain::enrollment::Key {
                version: req.version,
                expected_store_id: req.expected_store_id,
                pool_store_id: req.pool_store_id,
                request_id: req.request_id,
                fingerprint: req.fingerprint,
            };
            Ok(service
                .managed_enrollment_current(
                    grant,
                    bridge,
                    key,
                    req.member_id,
                    req.current_request_id,
                )
                .await?)
        }
        "_attempt_current_receipt_v1" => {
            let req: CurrentLookup = parse(params)?;
            version(req.version)?;
            let store = req.expected_store_id.clone();
            let route = grant.pool_route.clone();
            let id = req.receipt_id;
            let receipt = service
                .read(move |reader| reader.attempt_current_receipt(&store, &route, &id))
                .await?
                .ok_or_else(|| invalid("binding unavailable"))?;
            Ok(json!({"version":1,"store_id":req.expected_store_id,"receipt":receipt}))
        }
        "_attempt_admit_v1" => {
            for path in [
                "/binding",
                "/binding/attempt",
                "/binding/actor",
                "/binding/run_route",
                "/binding/bridge_route",
                "/consumed_ref",
                "/consumed_ref/bridge_route",
            ] {
                require_object(&params, path)?;
            }
            let req: Admit = parse(params)?;
            version(req.version)?;
            let request = AdmissionRequest {
                version: req.version,
                expected_store_id: req.expected_store_id.clone(),
                request_id: req.request_id,
                intent_id: req.intent_id,
                security_digest: req.security_digest,
                binding: req.binding,
                consumed_ref: req.consumed_ref,
                policy: req.policy,
                pool_route: grant.pool_route.clone(),
            };
            let record = service.attempt_admit_v1(grant, bridge, request).await?;
            let admission = record
                .admission
                .ok_or_else(|| invalid("binding unavailable"))?;
            Ok(
                json!({"version":1,"store_id":req.expected_store_id,"state":"admitted","admission":admission}),
            )
        }
        "_attempt_admission_lookup_v1" => {
            let req: Lookup = parse(params)?;
            version(req.version)?;
            let store = req.expected_store_id.clone();
            let route = grant.pool_route.clone();
            let id = req.intent_id;
            let digest = req.security_digest;
            let record = service
                .read(move |reader| reader.attempt_admission_lookup(&store, &route, &id, &digest))
                .await?;
            let (state, admission, outcome) = record
                .map_or(("absent".to_string(), None, None), |r| {
                    (r.state, r.admission, r.outcome_digest)
                });
            Ok(
                json!({"version":1,"store_id":req.expected_store_id,"state":state,"admission":admission,"outcome_digest":outcome}),
            )
        }
        "_attempt_admission_abort_v1" => {
            require_object(&params, "/pool_route")?;
            if params.get("pool_outcome_digest").is_none() {
                return Err(invalid("attempt request invalid").into());
            }
            let req: Abort = parse(params)?;
            version(req.version)?;
            if req.request_id != req.intent_id || req.pool_route != grant.pool_route {
                return Err(Error::Conflict.into());
            }
            let result = service
                .attempt_abort_v1(
                    grant,
                    pool,
                    req.expected_store_id.clone(),
                    req.intent_id,
                    req.security_digest,
                    req.pool_outcome_digest,
                )
                .await?;
            Ok(
                json!({"version":1,"store_id":req.expected_store_id,"state":result.state,"admission":result.admission,"outcome_digest":result.outcome_digest}),
            )
        }
        "_attempt_settle_v1" => {
            let req: Settle = parse(params)?;
            version(req.version)?;
            let result = service
                .attempt_settle_v1(
                    grant,
                    pool,
                    req.expected_store_id.clone(),
                    req.admission_id,
                    req.intent_id,
                    req.security_digest,
                    req.pool_outcome_digest,
                )
                .await?;
            Ok(
                json!({"version":1,"store_id":req.expected_store_id,"state":result.state,"admission":result.admission,"outcome_digest":result.outcome_digest}),
            )
        }
        "_attempt_retire_namespace_v1" => {
            require_object(&params, "/pool_route")?;
            require_object(&params, "/bridge_route")?;
            let req: Retire = parse(params)?;
            version(req.version)?;
            let request = NamespaceRequest {
                expected_store_id: req.expected_store_id.clone(),
                request_id: req.request_id,
                pool_route: req.pool_route,
                bridge_route: req.bridge_route,
                producer: req.producer,
                issuer_epoch: req.issuer_epoch,
                retirement_key: req.retirement_key,
                pool_seal_request_id: req.pool_seal_request_id,
            };
            let result = service
                .attempt_retire_namespace_v1(grant, bridge, pool, request.clone())
                .await?;
            Ok(
                json!({"version":1,"store_id":req.expected_store_id,"state":result.state,"bridge_route":request.bridge_route,"producer":request.producer,"issuer_epoch":request.issuer_epoch,"retirement_key":request.retirement_key,"retired_count":result.retired_count}),
            )
        }
        _ => Err(Error::Unsupported("attempt method unsupported".into()).into()),
    }
}
fn wire_id(value: &str) -> Result<()> {
    agent_run_domain::attempt_binding::validate_id(value)
}
/// Pool cannot pre-claim a currentness key that Run derives for enrollment.
fn reserved(request_id: &str) -> Result<()> {
    if agent_run_domain::batch::is_reserved_internal_key(request_id) {
        return Err(Error::Conflict);
    }
    Ok(())
}

/// Compare the bytes actually emitted by a reply with a closed wire shape.
#[cfg(test)]
pub(crate) fn assert_reply_keys(value: &Value, expected: &[&str]) {
    let bytes = serde_json::to_vec(value).unwrap();
    let decoded: Value = serde_json::from_slice(&bytes).unwrap();
    let actual: std::collections::BTreeSet<_> = decoded
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    let expected: std::collections::BTreeSet<_> = expected.iter().copied().collect();
    assert_eq!(actual, expected, "wire reply keys");
}

/// Configured peer readers, opened only by a method that performs their I/O.
/// Original-key enrollment lookup/abort and receipt replay never depend on the
/// Bridge reader credential or the Pool endpoint being available.
pub(crate) trait PeerSource: Send + Sync {
    fn bridge(&self) -> Result<Box<dyn BridgeReader + '_>>;
    fn pool(&self) -> Result<Box<dyn PoolReader + '_>>;
}

fn needs_bridge(method: &str) -> bool {
    matches!(
        method,
        "_attempt_current_v1"
            | "_attempt_admit_v1"
            | "_attempt_retire_namespace_v1"
            | "_managed_enrollment_current_v1"
    )
}

fn needs_pool(method: &str) -> bool {
    matches!(
        method,
        "_attempt_admission_abort_v1" | "_attempt_settle_v1" | "_attempt_retire_namespace_v1"
    )
}

/// Stands in for a peer that the selected method never contacts.
struct Unopened;
fn unopened<T>() -> crate::attempt_binding::PeerFuture<'static, T> {
    Box::pin(async { Err(Error::Integrity("attempt peer not opened".into())) })
}
impl BridgeReader for Unopened {
    fn context<'a>(
        &'a self,
        _: &'a IngressCurrent,
    ) -> crate::attempt_binding::PeerFuture<'a, crate::attempt_binding::BridgeContext> {
        unopened()
    }
    fn consume<'a>(
        &'a self,
        _: &'a AdmissionRequest,
    ) -> crate::attempt_binding::PeerFuture<'a, crate::attempt_binding::BridgeConsume> {
        unopened()
    }
    fn issuer<'a>(
        &'a self,
        _: &'a NamespaceRequest,
    ) -> crate::attempt_binding::PeerFuture<'a, crate::attempt_binding::BridgeIssuer> {
        unopened()
    }
    fn retire<'a>(
        &'a self,
        _: &'a NamespaceRequest,
    ) -> crate::attempt_binding::PeerFuture<'a, crate::attempt_binding::BridgeRetired> {
        unopened()
    }
}
impl PoolReader for Unopened {
    fn outcome<'a>(
        &'a self,
        _: &'a Route,
        _: &'a str,
        _: &'a str,
    ) -> crate::attempt_binding::PeerFuture<'a, crate::attempt_binding::PoolOutcome> {
        unopened()
    }
    fn seal<'a>(
        &'a self,
        _: &'a NamespaceRequest,
    ) -> crate::attempt_binding::PeerFuture<'a, agent_run_store::attempt_binding::PoolSeal> {
        unopened()
    }
}

/// Open only the peers this method contacts; a configuration or credential
/// failure becomes the closed operator-recovery error, never a dropped socket.
async fn dispatch_with_peers(
    service: &Service,
    grant: &PeerGrant,
    peers: &dyn PeerSource,
    method: &str,
    params: Value,
) -> std::result::Result<Value, DispatchFailure> {
    let unavailable = |_| invalid("attempt peer configuration unavailable");
    let bridge: Box<dyn BridgeReader + '_> = if needs_bridge(method) {
        peers.bridge().map_err(unavailable)?
    } else {
        Box::new(Unopened)
    };
    let pool: Box<dyn PoolReader + '_> = if needs_pool(method) {
        peers.pool().map_err(unavailable)?
    } else {
        Box::new(Unopened)
    };
    dispatch(
        service,
        grant,
        bridge.as_ref(),
        pool.as_ref(),
        method,
        params,
    )
    .await
}

/// Serve one bounded frame after same-UID and executable/build authentication.
/// The caller owns endpoint lifecycle and must not pass request-selected pins.
pub(crate) async fn serve_one(
    stream: UnixStream,
    pin: PeerPin,
    service: &Service,
    grant: &PeerGrant,
    expected_transport_pin: &str,
    peers: &dyn PeerSource,
) -> Result<()> {
    let admitted_pin = pin.clone();
    let home = service.home.clone();
    let (stream, verified) = tokio::task::spawn_blocking(move || {
        let result = verify_peer(&stream, &pin)
            .and_then(|()| crate::attempt_transport::verify_pool_resident(&stream, &home));
        (stream, result)
    })
    .await
    .map_err(|_| invalid("attempt peer unavailable"))?;
    if verified.is_err() {
        return reject_pool_peer(stream, service).await;
    }
    let (read, mut write) = stream.into_split();
    let mut bytes = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        BufReader::new(read)
            .take(1024 * 1024 + 1)
            .read_until(b'\n', &mut bytes),
    )
    .await
    .map_err(|_| invalid("attempt peer unavailable"))??;
    if bytes.len() > 1024 * 1024 || bytes.pop() != Some(b'\n') {
        return Err(invalid("attempt frame invalid"));
    }
    let request: Envelope = parse(strict(&bytes)?)?;
    wire_id(&request.id)?;
    wire_id(&request.method)?;
    let store = service
        .read(|reader| agent_run_store::store_identity::read(&reader.conn))
        .await?
        .to_string();
    // A client can connect and then hold a frame while the operator re-pins
    // Pool or Bridge. Re-read the live pin after the full frame, before any
    // dispatch, so a stale pre-upgrade connection cannot create new authority.
    let current = service.current_provider_config().and_then(|(config, _)| {
        let current_pin = agent_run_store::managed_enrollment::Grant::from_config(&config)
            .map_err(|_| peer_not_authorized())?;
        if current_pin.transport_pin_sha256 != expected_transport_pin {
            return Err(peer_not_authorized());
        }
        let selected = config
            .managed_attempt_binding
            .as_ref()
            .ok_or_else(peer_not_authorized)?;
        let transport = selected
            .transport
            .as_ref()
            .ok_or_else(peer_not_authorized)?;
        if selected.pool_resident_executable.as_ref() != Some(&admitted_pin.executable)
            || selected.pool_resident_sha256.as_ref() != Some(&admitted_pin.sha256)
            || transport.run_route.worker_uid != admitted_pin.uid
            || transport.run_route != grant.run_route
            || transport.pool_route != grant.pool_route
            || transport.bridge_route != grant.bridge_route
            || transport.producer != grant.producer
            || selected.host_id != grant.host_id
        {
            return Err(peer_not_authorized());
        }
        Ok(())
    });
    let response = match current {
        // An authenticated resident learns that this Run home is not the
        // configured store from the closed envelope, not from a closed socket.
        Ok(()) if grant.run_route.component_store_id != store => {
            Err(DispatchFailure::from(Error::StoreIdentityMismatch))
        }
        Ok(()) => dispatch_with_peers(service, grant, peers, &request.method, request.params).await,
        Err(_) => Err(DispatchFailure::from(peer_not_authorized())),
    };
    let reply = match response {
        Ok(result) => json!({"id":request.id,"result":result}),
        Err(error) => {
            json!({"id":request.id,"error":dispatch_error(&error,&request.method,&store)})
        }
    };
    let mut bytes = serde_json::to_vec(&reply)?;
    if bytes.len() + 1 > 1024 * 1024 {
        return Err(invalid("attempt response too large"));
    }
    bytes.push(b'\n');
    tokio::time::timeout(std::time::Duration::from_secs(5), write.write_all(&bytes))
        .await
        .map_err(|_| invalid("attempt peer unavailable"))??;
    Ok(())
}

/// Bridge receipt and custody reads use separately granted principals. The
/// selected build comes only from local configuration and the peer kernel identity.
async fn serve_bridge_private_one(
    stream: UnixStream,
    pin: PeerPin,
    service: &Service,
) -> Result<()> {
    let stream = tokio::task::spawn_blocking(move || {
        verify_peer(&stream, &pin)?;
        Ok::<_, Error>((stream, pin))
    })
    .await
    .map_err(|_| invalid("attempt peer unavailable"))??;
    let (stream, pin) = stream;
    let (read, mut write) = stream.into_split();
    let mut bytes = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        BufReader::new(read)
            .take(1024 * 1024 + 1)
            .read_until(b'\n', &mut bytes),
    )
    .await
    .map_err(|_| invalid("attempt peer unavailable"))??;
    if bytes.len() > 1024 * 1024 || bytes.pop() != Some(b'\n') {
        return Err(invalid("attempt frame invalid"));
    }
    let request: Envelope = parse(strict(&bytes)?)?;
    wire_id(&request.id)?;
    // A revocation or route change while a peer was sending the frame takes
    // effect before any lookup or mutation.
    let (config, _) = service.current_provider_config()?;
    let managed = config
        .managed_attempt_binding
        .ok_or_else(|| Error::Unsupported("managed attempt binding unavailable".into()))?;
    let store = service
        .read(|reader| agent_run_store::store_identity::read(&reader.conn))
        .await?;
    let effect_method = matches!(
        request.method.as_str(),
        "_owner_effect_lookup_v2" | "_owner_effect_abort_v2"
    );
    let result = async {
        match request.method.as_str() {
            "_attempt_current_receipt_v1" => {
                let reader = managed
                    .bridge_receipt_readers
                    .iter()
                    .find(|r| {
                        r.worker_uid == pin.uid
                            && r.executable == pin.executable
                            && r.executable_sha256 == pin.sha256
                    })
                    .ok_or_else(peer_not_authorized)?;
                if reader.run_route.component_store_id != store.to_string() {
                    return Err(Error::StoreIdentityMismatch);
                }
                let parsed: CurrentLookup = parse(request.params)?;
                version(parsed.version)?;
                if parsed.expected_store_id != store {
                    return Err(Error::StoreIdentityMismatch);
                }
                let route = reader.pool_route.clone();
                let expected = store.clone();
                let id = parsed.receipt_id;
                let receipt = service
                    .read(move |db| db.attempt_current_receipt(&expected, &route, &id))
                    .await?
                    .ok_or_else(|| invalid("binding unavailable"))?;
                Ok(json!({"version":1,"store_id":store,"receipt":receipt}))
            }
            "_owner_effect_lookup_v2" | "_owner_effect_abort_v2" => {
                let reader = managed
                    .bridge_custody_readers
                    .iter()
                    .find(|r| {
                        r.worker_uid == pin.uid
                            && r.executable == pin.executable
                            && r.executable_sha256 == pin.sha256
                    })
                    .ok_or_else(peer_not_authorized)?;
                let parsed: EffectLookup = parse(request.params)?;
                if parsed.version != 2 {
                    return Err(Error::Unsupported("attempt version unsupported".into()));
                }
                if parsed.expected_store_id != store
                    || reader.run_route.component_store_id != store.to_string()
                {
                    return Err(Error::StoreIdentityMismatch);
                }
                if !reader.authorizes_scope(&parsed.producer_scope) {
                    return Err(peer_not_authorized());
                }
                let route = agent_run_store::owner_v2::EffectRoute {
                    run_route: reader.run_route.clone(),
                    bridge_route: reader.bridge_route.clone(),
                };
                let view = if request.method == "_owner_effect_abort_v2" {
                    service
                        .lane("owner-effect-recovery")?
                        .local(
                            agent_run_store::ops::AbortOwnerEffect {
                                store: store.clone(),
                                producer_scope: parsed.producer_scope,
                                effect_id: parsed.effect_id,
                                effect_digest: parsed.effect_digest,
                                route,
                            },
                            crate::broker_writer::BROKER_BUDGET,
                        )
                        .await?
                } else {
                    let expected = store.clone();
                    service
                        .read(move |db| {
                            db.owner_effect_lookup(
                                &expected,
                                &parsed.producer_scope,
                                &parsed.effect_id,
                                &parsed.effect_digest,
                                &route,
                            )
                        })
                        .await?
                };
                let mut value = serde_json::to_value(view)?;
                let object = value
                    .as_object_mut()
                    .ok_or_else(|| invalid("owner effect unavailable"))?;
                object.insert("version".into(), json!(2));
                object.insert("store_id".into(), json!(store));
                Ok(value)
            }
            _ => Err(peer_not_authorized()),
        }
    }
    .await;
    let reply = match result {
        Ok(value) => json!({"id":request.id,"result":value}),
        Err(error) => {
            let mut detail = closed_error(&error, &request.method, &store.to_string());
            if effect_method {
                detail["version"] = json!(2);
            }
            json!({"id":request.id,"error":detail})
        }
    };
    let mut bytes = serde_json::to_vec(&reply)?;
    if bytes.len() + 1 > 1024 * 1024 {
        return Err(invalid("attempt response too large"));
    }
    bytes.push(b'\n');
    tokio::time::timeout(std::time::Duration::from_secs(5), write.write_all(&bytes))
        .await
        .map_err(|_| invalid("attempt peer unavailable"))??;
    Ok(())
}

pub(crate) fn peer_not_authorized() -> Error {
    Error::Broker {
        message: "attempt peer refused".into(),
        broker_error_code: Some("peer_not_authorized".into()),
        broker_error_data: Some(json!({"recovery":"none"})),
    }
}

/// A rejected same-UID peer receives only a bounded, typed denial. The request
/// ID is echoed when the submitted frame itself is well formed; no store or
/// credential is opened and no request can reach dispatch.
async fn reject_pool_peer(stream: UnixStream, service: &Service) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut bytes = Vec::new();
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        BufReader::new(read)
            .take(1024 * 1024 + 1)
            .read_until(b'\n', &mut bytes),
    )
    .await;
    let id = if read.is_ok() && bytes.len() <= 1024 * 1024 && bytes.pop() == Some(b'\n') {
        strict(&bytes)
            .ok()
            .and_then(|value| value.get("id").cloned())
            .filter(|id| id.as_str().is_some_and(|s| wire_id(s).is_ok()))
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let mut error = json!({"version":1,"code":"peer_not_authorized","recovery":"none"});
    if let Ok(store) = service
        .read(|reader| agent_run_store::store_identity::read(&reader.conn))
        .await
    {
        error["store_id"] = json!(store);
    }
    let mut reply = serde_json::to_vec(&json!({"id":id,"error":error}))?;
    reply.push(b'\n');
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), write.write_all(&reply)).await;
    Err(peer_not_authorized())
}

/// Serve one configured same-UID Bridge reader; equal receipt and custody pins
/// may share the process, but each method still requires its own grant.
pub async fn serve_bridge_receipt(stream: UnixStream, service: &Service) -> Result<()> {
    let (config, _) = service.current_provider_config()?;
    let managed = config
        .managed_attempt_binding
        .ok_or_else(|| Error::Unsupported("managed attempt binding unavailable".into()))?;
    let mut pins: Vec<PeerPin> = Vec::new();
    for (uid, executable, sha256) in managed
        .bridge_receipt_readers
        .into_iter()
        .map(|r| (r.worker_uid, r.executable, r.executable_sha256))
        .chain(
            managed
                .bridge_custody_readers
                .into_iter()
                .map(|r| (r.worker_uid, r.executable, r.executable_sha256)),
        )
    {
        if !pins
            .iter()
            .any(|p| p.uid == uid && p.executable == executable && p.sha256 == sha256)
        {
            pins.push(PeerPin {
                uid,
                executable,
                sha256,
            });
        }
    }
    let (stream, pin) = tokio::task::spawn_blocking(move || {
        let mut selected = None;
        for pin in pins {
            if verify_peer(&stream, &pin).is_ok() {
                if selected.is_some() {
                    return Err(Error::Conflict);
                }
                selected = Some(pin);
            }
        }
        Ok::<_, Error>((stream, selected.ok_or(Error::Conflict)?))
    })
    .await
    .map_err(|_| invalid("attempt peer unavailable"))??;
    serve_bridge_private_one(stream, pin, service).await
}

/// Peer readers resolved only from current local configuration.
struct ConfiguredPeers {
    home: std::path::PathBuf,
    pool_socket: std::path::PathBuf,
    pool_pin: PeerPin,
    pool_route: Route,
    bridge_socket: std::path::PathBuf,
    bridge_pin: PeerPin,
    bridge_route: Route,
    credential_file: std::path::PathBuf,
    credential_sha256: String,
}
impl PeerSource for ConfiguredPeers {
    fn bridge(&self) -> Result<Box<dyn BridgeReader + '_>> {
        use crate::attempt_transport::{AuthenticatedBridge, Endpoint};
        Ok(Box::new(AuthenticatedBridge::from_pinned_file(
            Endpoint::configured(self.bridge_socket.clone(), self.bridge_pin.clone())?,
            self.bridge_route.clone(),
            &self.credential_file,
            &self.credential_sha256,
        )?))
    }
    fn pool(&self) -> Result<Box<dyn PoolReader + '_>> {
        use crate::attempt_transport::{AuthenticatedPool, Endpoint};
        Ok(Box::new(AuthenticatedPool::configured(
            Endpoint::configured(self.pool_socket.clone(), self.pool_pin.clone())?,
            self.pool_route.clone(),
            self.home.clone(),
        )?))
    }
}

/// Serve one configured Pool resident on the separate R2 socket. Current
/// configuration supplies all peer identities; no request selects a route or
/// credential. Disabled configurations preserve only exact-key recovery.
pub async fn serve_pool_attempt(stream: UnixStream, service: &Service) -> Result<()> {
    let (config, _) = service.current_provider_config()?;
    let transport_pin =
        agent_run_store::managed_enrollment::Grant::from_config(&config)?.transport_pin_sha256;
    let settings = config
        .managed_attempt_binding
        .ok_or_else(|| Error::Unsupported("managed attempt binding unavailable".into()))?;
    let transport = settings
        .transport
        .as_ref()
        .ok_or_else(|| Error::Unsupported("managed attempt transport unavailable".into()))?;
    let pool_executable = settings
        .pool_resident_executable
        .clone()
        .ok_or_else(|| invalid("managed Pool pin unavailable"))?;
    let pool_hash = settings
        .pool_resident_sha256
        .clone()
        .ok_or_else(|| invalid("managed Pool pin unavailable"))?;
    let uid = transport.run_route.worker_uid;
    let peer_pin = PeerPin {
        uid,
        executable: pool_executable.clone(),
        sha256: pool_hash.clone(),
    };
    let pool_pin = PeerPin {
        uid,
        executable: pool_executable,
        sha256: pool_hash,
    };
    let bridge_pin = PeerPin {
        uid,
        executable: transport.bridge_executable.clone(),
        sha256: transport.bridge_executable_sha256.clone(),
    };
    let auth_pin = PeerPin {
        uid: peer_pin.uid,
        executable: peer_pin.executable.clone(),
        sha256: peer_pin.sha256.clone(),
    };
    let home = service.home.clone();
    let (stream, verified) = tokio::task::spawn_blocking(move || {
        let result = verify_peer(&stream, &auth_pin)
            .and_then(|()| crate::attempt_transport::verify_pool_resident(&stream, &home));
        (stream, result)
    })
    .await
    .map_err(|_| invalid("attempt peer unavailable"))?;
    if verified.is_err() {
        return reject_pool_peer(stream, service).await;
    }
    let peers = ConfiguredPeers {
        home: service.home.clone(),
        pool_socket: transport.pool_socket.clone(),
        pool_pin,
        pool_route: transport.pool_route.clone(),
        bridge_socket: transport.bridge_socket.clone(),
        bridge_pin,
        bridge_route: transport.bridge_route.clone(),
        credential_file: transport.bridge_reader_credential_file.clone(),
        credential_sha256: transport.bridge_reader_credential_sha256.clone(),
    };
    let grant = PeerGrant {
        enabled: settings.enabled,
        pool_route: transport.pool_route.clone(),
        run_route: transport.run_route.clone(),
        bridge_route: transport.bridge_route.clone(),
        producer: transport.producer.clone(),
        host_id: settings.host_id.clone(),
    };
    serve_one(stream, peer_pin, service, &grant, &transport_pin, &peers).await
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::attempt_binding::{
        BridgeConsume, BridgeContext, BridgeIssuer, BridgeRetired, PeerFuture, PoolOutcome,
    };
    use agent_run_store::attempt_binding::PoolSeal;
    struct Fake;
    #[test]
    fn typed_attempt_refusals_keep_recovery_contract() {
        for (message, code, recovery) in [
            ("attempt not ready", "attempt_not_ready", "same_key_lookup"),
            ("attempt mismatch", "attempt_mismatch", "none"),
            (
                "attempt peer executable changed",
                "attempt_mismatch",
                "none",
            ),
            (
                "attempt peer verification failed",
                "peer_not_authorized",
                "none",
            ),
            ("actor mismatch", "actor_mismatch", "none"),
            ("invalid attempt actor", "actor_mismatch", "none"),
            ("route mismatch", "route_mismatch", "none"),
            ("binding stale", "binding_stale", "fresh_invocation"),
            (
                "assertion historical",
                "assertion_historical",
                "fresh_invocation",
            ),
        ] {
            let result = closed_error(&invalid(message), "_attempt_admit_v1", "store");
            assert_reply_keys(&result, &["version", "store_id", "code", "recovery"]);
            assert_eq!(result["code"], code);
            assert_eq!(result["recovery"], recovery);
        }
    }
    /// Pool cannot pre-claim the currentness key Run derives for enrollment.
    #[tokio::test]
    async fn current_request_ids_cannot_use_reserved_internal_prefixes() {
        let home = tempfile::tempdir().unwrap();
        let service = Service::new(home.path().to_path_buf());
        let route = |store: &str, id: &str| Route {
            component_store_id: store.into(),
            route_id: id.into(),
            worker_uid: 1,
        };
        let grant = PeerGrant {
            enabled: true,
            pool_route: route("00000000-0000-4000-8000-000000000041", "pool"),
            run_route: route("store_0123456789abcdef0123456789abcdef", "run"),
            bridge_route: route("00000000-0000-4000-8000-000000000042", "bridge"),
            producer: "pool".into(),
            host_id: "host".into(),
        };
        for (purpose, request_id) in [("egress", "p3c_0123"), ("egress", "p3_0123")] {
            let params = json!({"version":1,"expected_store_id":"store_0123456789abcdef0123456789abcdef","request_id":request_id,"lineage_root_id":"ag-20261109-120000-0000000001","bound_execution_id":"ag-20261109-120000-0000000001","purpose":purpose});
            let error = dispatch(
                &service,
                &grant,
                &Fake,
                &Fake,
                "_attempt_current_v1",
                params,
            )
            .await
            .unwrap_err();
            let reply = dispatch_error(&error, "_attempt_current_v1", "store");
            assert_eq!(
                (reply["code"].as_str(), reply["recovery"].as_str()),
                (Some("intent_conflict"), Some("none"))
            );
        }
        let params = json!({"version":1,"expected_store_id":"store_0123456789abcdef0123456789abcdef","request_id":"p3c_0123","observation_ref":"context","invocation_key":"invocation","purpose":"ingress"});
        let error = dispatch(
            &service,
            &grant,
            &Fake,
            &Fake,
            "_attempt_current_v1",
            params,
        )
        .await
        .unwrap_err();
        assert_eq!(
            dispatch_error(&error, "_attempt_current_v1", "store")["code"],
            "intent_conflict"
        );
    }
    impl BridgeReader for Fake {
        fn context<'a>(&'a self, _: &'a IngressCurrent) -> PeerFuture<'a, BridgeContext> {
            Box::pin(async { Err(Error::Conflict) })
        }
        fn consume<'a>(&'a self, _: &'a AdmissionRequest) -> PeerFuture<'a, BridgeConsume> {
            Box::pin(async { Err(Error::Conflict) })
        }
        fn issuer<'a>(&'a self, _: &'a NamespaceRequest) -> PeerFuture<'a, BridgeIssuer> {
            Box::pin(async { Err(Error::Conflict) })
        }
        fn retire<'a>(&'a self, _: &'a NamespaceRequest) -> PeerFuture<'a, BridgeRetired> {
            Box::pin(async { Err(Error::Conflict) })
        }
    }
    impl PeerSource for Fake {
        fn bridge(&self) -> Result<Box<dyn BridgeReader + '_>> {
            Ok(Box::new(Fake))
        }
        fn pool(&self) -> Result<Box<dyn PoolReader + '_>> {
            Ok(Box::new(Fake))
        }
    }
    impl PoolReader for Fake {
        fn outcome<'a>(
            &'a self,
            _: &'a Route,
            _: &'a str,
            _: &'a str,
        ) -> PeerFuture<'a, PoolOutcome> {
            Box::pin(async { Err(Error::Conflict) })
        }
        fn seal<'a>(&'a self, _: &'a NamespaceRequest) -> PeerFuture<'a, PoolSeal> {
            Box::pin(async { Err(Error::Conflict) })
        }
    }
    /// Real private Unix socket with an isolated fake same-UID Pool process.
    /// Disabled discovery is empty and no external service is involved.
    #[tokio::test]
    async fn private_attempt_wire_authenticates_peer_and_keeps_default_off() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = tempfile::Builder::new()
            .prefix("r2-wire-")
            .tempdir_in(base)
            .unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let store_home = tempfile::tempdir().unwrap();
        let store = agent_run_store::fixture::initialize(store_home.path()).unwrap();
        let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
        drop(store);
        let service = Service::new(store_home.path().to_path_buf());
        let socket = dir.path().join("r.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let executable = crate::attempt_transport::test_peer::copy_peer(dir.path());
        let digest = agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap());
        crate::attempt_transport::test_peer::wait_build_age(&executable).await;
        let output = dir.path().join("reply");
        let frame = json!({"id":"one","method":"_attempt_capabilities_v1","params":{"version":1}})
            .to_string();
        let mut child =
            crate::attempt_transport::test_peer::child_at(&executable, "client", &socket)
                .env("RUN_R2_PEER_FRAME", frame)
                .env("RUN_R2_PEER_OUTPUT", &output)
                .spawn()
                .unwrap();
        let (stream, _) =
            tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
        // SAFETY: geteuid has no memory arguments or side effects.
        let uid = unsafe { libc::geteuid() };
        let route = |store: &str, id: &str| Route {
            component_store_id: store.into(),
            route_id: id.into(),
            worker_uid: uid,
        };
        let grant = PeerGrant {
            enabled: false,
            pool_route: route("pool", "pool"),
            run_route: route(&sid.to_string(), "run"),
            bridge_route: route("00000000-0000-4000-8000-000000000001", "bridge"),
            producer: "pool".into(),
            host_id: "host".into(),
        };
        let pin = PeerPin {
            uid,
            executable,
            sha256: digest,
        };
        let config = format!(
            "schema_version=2\n[managed_attempt_binding]\nenabled=false\nrole_name='managed'\npool_server='pool'\npool_executable_sha256='{}'\nbridge_hook_sha256='{}'\nhost_id='host'\npool_resident_executable='{}'\npool_resident_sha256='{}'\n[managed_attempt_binding.transport]\nrun_route={{component_store_id='{sid}',route_id='run',worker_uid={uid}}}\npool_route={{component_store_id='pool',route_id='pool',worker_uid={uid}}}\nbridge_route={{component_store_id='00000000-0000-4000-8000-000000000001',route_id='bridge',worker_uid={uid}}}\nproducer='pool'\npool_socket='{}/pool.sock'\nbridge_socket='{}/bridge.sock'\nbridge_executable='/usr/bin/false'\nbridge_executable_sha256='{}'\nbridge_reader_credential_file='{}/reader'\nbridge_reader_credential_sha256='{}'\n",
            "a".repeat(64),
            "c".repeat(64),
            pin.executable.display(),
            pin.sha256,
            dir.path().display(),
            dir.path().display(),
            "d".repeat(64),
            dir.path().display(),
            "e".repeat(64)
        );
        std::fs::write(store_home.path().join("config.toml"), &config).unwrap();
        let transport_pin = agent_run_store::managed_enrollment::Grant::from_config(
            &agent_run_config::provider_config::ProviderConfig::load(store_home.path())
                .unwrap()
                .0,
        )
        .unwrap()
        .transport_pin_sha256;
        serve_one(stream, pin.clone(), &service, &grant, &transport_pin, &Fake)
            .await
            .unwrap();
        assert!(child.wait().await.unwrap().success());
        let reply: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(reply["id"], "one");
        assert_eq!(reply["result"]["store_id"], sid.to_string());
        assert_eq!(reply["result"]["capabilities"], json!([]));
        assert_reply_keys(
            &reply["result"],
            &["version", "store_id", "capabilities", "policy"],
        );

        // A connected old build cannot wait out an operator re-pin and then
        // submit a frame under the newly configured resident identity.
        for (suffix, changed) in [
            (
                "resident",
                config.replace(
                    &format!("pool_resident_sha256='{}'", pin.sha256),
                    &format!("pool_resident_sha256='{}'", "f".repeat(64)),
                ),
            ),
            ("bridge", config.replace("bridge.sock", "bridge-new.sock")),
        ] {
            std::fs::write(store_home.path().join("config.toml"), &config).unwrap();
            let ready = dir.path().join(format!("{suffix}-ready"));
            let release = dir.path().join(format!("{suffix}-release"));
            let output = dir.path().join(format!("{suffix}-reply"));
            let mut delayed = crate::attempt_transport::test_peer::child_at(
                &pin.executable,
                "delayed-client",
                &socket,
            )
            .env("RUN_R2_PEER_READY", &ready)
            .env("RUN_R2_PEER_RELEASE", &release)
            .env(
                "RUN_R2_PEER_FRAME",
                json!({"id":"stale","method":"_attempt_capabilities_v1","params":{"version":1}})
                    .to_string(),
            )
            .env("RUN_R2_PEER_OUTPUT", &output)
            .spawn()
            .unwrap();
            let (stream, _) =
                tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
            crate::attempt_transport::test_peer::wait_file(&ready).await;
            let runner = tokio::spawn({
                let service = service.clone();
                let grant = grant.clone();
                let pin = pin.clone();
                let transport_pin = transport_pin.clone();
                async move { serve_one(stream, pin, &service, &grant, &transport_pin, &Fake).await }
            });
            assert_ne!(changed, config);
            std::fs::write(store_home.path().join("config.toml"), changed).unwrap();
            std::fs::write(release, b"release").unwrap();
            runner.await.unwrap().unwrap();
            assert!(delayed.wait().await.unwrap().success());
            let reply: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
            assert_eq!(reply["error"]["code"], "peer_not_authorized");
            assert_reply_keys(
                &reply["error"],
                &["version", "store_id", "code", "recovery"],
            );
        }
    }

    /// Exact-key enrollment lookup/abort through the real resident endpoint
    /// never needs the Bridge reader credential; a method that does need it,
    /// and a wrong configured store, reply with the closed operator envelope.
    #[tokio::test]
    async fn pool_recovery_does_not_open_bridge_credential() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = tempfile::Builder::new()
            .prefix("r2-lazy-")
            .tempdir_in(base)
            .unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let store_home = tempfile::tempdir().unwrap();
        let store = agent_run_store::fixture::initialize(store_home.path()).unwrap();
        let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
        drop(store);
        let writer = crate::writer_test_harness::Harness::start(store_home.path());
        let service = Service::new(store_home.path().to_path_buf()).with_writer(writer.broker());
        let socket = dir.path().join("r.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let executable = crate::attempt_transport::test_peer::copy_peer(dir.path());
        let digest = agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap());
        crate::attempt_transport::test_peer::wait_build_age(&executable).await;
        // SAFETY: geteuid has no memory arguments or side effects.
        let uid = unsafe { libc::geteuid() };
        let pool_store = "00000000-0000-4000-8000-000000000031";
        // The configured credential file does not exist.
        let config = |run_store: &str| {
            format!(
                "schema_version=2\n[managed_attempt_binding]\nenabled=false\nrole_name='managed'\npool_server='pool'\npool_executable_sha256='{}'\nbridge_hook_sha256='{}'\nhost_id='host'\npool_resident_executable='{}'\npool_resident_sha256='{digest}'\n[managed_attempt_binding.transport]\nrun_route={{component_store_id='{run_store}',route_id='run',worker_uid={uid}}}\npool_route={{component_store_id='{pool_store}',route_id='pool',worker_uid={uid}}}\nbridge_route={{component_store_id='00000000-0000-4000-8000-000000000032',route_id='bridge',worker_uid={uid}}}\nproducer='pool'\npool_socket='{}/pool.sock'\nbridge_socket='{}/bridge.sock'\nbridge_executable='/usr/bin/false'\nbridge_executable_sha256='{}'\nbridge_reader_credential_file='{}/missing-reader'\nbridge_reader_credential_sha256='{}'\n",
                "a".repeat(64),
                "c".repeat(64),
                executable.display(),
                dir.path().display(),
                dir.path().display(),
                "d".repeat(64),
                dir.path().display(),
                "e".repeat(64)
            )
        };
        let call = |method: &str, params: Value| {
            let frame = json!({"id":"lazy","method":method,"params":params}).to_string();
            let output = dir.path().join(format!("{method}-reply"));
            let child =
                crate::attempt_transport::test_peer::child_at(&executable, "client", &socket)
                    .env("RUN_R2_PEER_FRAME", frame)
                    .env("RUN_R2_PEER_OUTPUT", &output)
                    .spawn()
                    .unwrap();
            (child, output)
        };
        let key = json!({"version":1,"expected_store_id":sid,"pool_store_id":pool_store,"request_id":"lazy-1","fingerprint":"a".repeat(64)});
        let mut current = key.clone();
        current["member_id"] = json!("member");
        current["current_request_id"] = json!("observe-1");
        std::fs::write(
            store_home.path().join("config.toml"),
            config(&sid.to_string()),
        )
        .unwrap();
        let mut replies = Vec::new();
        for (method, params) in [
            ("_managed_start_lookup_v1", key.clone()),
            ("_managed_start_abort_v1", key.clone()),
            ("_managed_enrollment_current_v1", current),
        ] {
            let (mut child, output) = call(method, params);
            let (stream, _) =
                tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
            serve_pool_attempt(stream, &service).await.unwrap();
            assert!(child.wait().await.unwrap().success());
            replies.push(serde_json::from_slice::<Value>(&std::fs::read(output).unwrap()).unwrap());
        }
        assert_eq!(replies[0]["result"]["state"], "absent");
        assert_eq!(replies[1]["result"]["state"], "aborted");
        assert_reply_keys(
            &replies[1]["result"],
            &[
                "version",
                "store_id",
                "request_id",
                "fingerprint",
                "state",
                "receipt",
                "jobs_state",
            ],
        );
        assert_eq!(replies[2]["error"]["code"], "recovery_required");
        assert_eq!(replies[2]["error"]["recovery"], "operator");
        assert_reply_keys(
            &replies[2]["error"],
            &["version", "store_id", "code", "recovery"],
        );
        // A configured Run route naming another store is a typed refusal too.
        std::fs::write(
            store_home.path().join("config.toml"),
            config(&format!("store_{}", "f".repeat(32))),
        )
        .unwrap();
        let (mut child, output) = call("_managed_start_lookup_v1", key);
        let (stream, _) =
            tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
        serve_pool_attempt(stream, &service).await.unwrap();
        assert!(child.wait().await.unwrap().success());
        let reply: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(reply["error"]["code"], "recovery_required");
        assert_eq!(reply["error"]["store_id"], sid.to_string());
        assert_reply_keys(
            &reply["error"],
            &["version", "store_id", "code", "recovery"],
        );
    }

    /// Bridge has a distinct read-only grant, and the route comes from that
    /// grant rather than an untrusted field in the request.
    #[tokio::test]
    async fn bridge_receipt_reader_is_pinned_and_read_only() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = tempfile::Builder::new()
            .prefix("r2-bridge-")
            .tempdir_in(base)
            .unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let home = tempfile::tempdir().unwrap();
        let store = agent_run_store::fixture::initialize(home.path()).unwrap();
        let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
        let id = AgentId::new();
        let launch = uuid::Uuid::new_v4().to_string();
        let receipt_id = uuid::Uuid::new_v4().to_string();
        // SAFETY: geteuid is read-only and has no preconditions.
        let uid = unsafe { libc::geteuid() };
        let route = Route {
            component_store_id: "pool-store".into(),
            route_id: "pool-route".into(),
            worker_uid: uid,
        };
        let receipt = json!({"receipt_id":receipt_id,"purpose":"ingress","lineage_root_id":id,"bound_execution_id":id,"attempt":{"run_store_id":sid,"lineage_root_id":id,"execution_id":id,"sequence":1,"attempt_id":"attempt","launch_id":launch},"incarnation":{"host_id":"host","boot_id":"00000000-0000-4000-8000-000000000001","pid_namespace_inode":1,"uid":uid,"pid":2,"start_ticks":1},"actor":{"address":"host/codex/s","role":"root","native_id":"s","epoch":1},"successor_proof":{"lineage_root_id":id,"bound_execution_id":id,"current_execution_id":id,"chain":[{"execution_id":id,"parent_execution_id":null,"sequence":1}]}});
        store.conn.execute("INSERT INTO attempt_current_receipts(request_id,pool_route_json,fingerprint,receipt_id,receipt_json) VALUES(?,?,?,?,?)",rusqlite::params!["request",serde_json::to_string(&route).unwrap(),"a".repeat(64),receipt_id,receipt.to_string()]).unwrap();
        drop(store);
        let executable = crate::attempt_transport::test_peer::copy_peer(dir.path());
        let digest = agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap());
        let config = format!(
            r#"schema_version=2
[managed_attempt_binding]
enabled=true
role_name="managed"
pool_server="pool"
pool_executable_sha256="{pool_hash}"
bridge_hook_sha256="{hook_hash}"
host_id="host"
pool_resident_executable="/usr/bin/false"
pool_resident_sha256="{resident_hash}"
[[managed_attempt_binding.bridge_receipt_readers]]
reader_id="bridge"
worker_uid={uid}
executable="{python}"
executable_sha256="{digest}"
run_route={{component_store_id="{sid}",route_id="run",worker_uid={uid}}}
pool_route={{component_store_id="pool-store",route_id="pool-route",worker_uid={uid}}}
"#,
            pool_hash = "a".repeat(64),
            hook_hash = "c".repeat(64),
            resident_hash = "b".repeat(64),
            python = executable.display()
        );
        std::fs::write(home.path().join("config.toml"), &config).unwrap();
        let service = Service::new(home.path().to_path_buf());
        let socket = dir.path().join("read.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let request=json!({"id":"lookup","method":"_attempt_current_receipt_v1","params":{"version":1,"expected_store_id":sid,"receipt_id":receipt_id}}).to_string();
        crate::attempt_transport::test_peer::wait_build_age(&executable).await;
        let output = dir.path().join("reply");
        let mut child =
            crate::attempt_transport::test_peer::child_at(&executable, "client", &socket)
                .env("RUN_R2_PEER_FRAME", request)
                .env("RUN_R2_PEER_OUTPUT", &output)
                .spawn()
                .unwrap();
        let (stream, _) =
            tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
        serve_bridge_receipt(stream, &service).await.unwrap();
        assert!(child.wait().await.unwrap().success());
        let value: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(value["result"]["receipt"], receipt);
        assert_reply_keys(&value["result"], &["version", "store_id", "receipt"]);
    }

    /// A separately pinned Bridge principal can recover only its configured
    /// effect scope. Abort is durable even after the managed switch is off.
    #[tokio::test]
    async fn bridge_custody_lookup_abort_is_scoped_and_writer_fenced() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = tempfile::Builder::new()
            .prefix("r3-custody-")
            .tempdir_in(base)
            .unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let home = tempfile::tempdir().unwrap();
        let store = agent_run_store::fixture::initialize(home.path()).unwrap();
        let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
        // SAFETY: geteuid is read-only and has no preconditions.
        let uid = unsafe { libc::geteuid() };
        let run_route = Route {
            component_store_id: sid.to_string(),
            route_id: "run".into(),
            worker_uid: uid,
        };
        let bridge_route = Route {
            component_store_id: "00000000-0000-4000-8000-000000000001".into(),
            route_id: "bridge".into(),
            worker_uid: uid,
        };
        store.conn.execute("INSERT INTO owner_effects(producer_scope,effect_id,effect_digest,run_route_json,bridge_route_json,outbox_json,dispatch_token,state,protocol,native_id) VALUES(?,?,?,?,?,'{}','token','accepted','codex_turn','turn-id')",rusqlite::params!["pool/store/pool/member","confirmed","d".repeat(64),serde_json::to_string(&run_route).unwrap(),serde_json::to_string(&bridge_route).unwrap()]).unwrap();
        drop(store);
        let executable = crate::attempt_transport::test_peer::copy_peer(dir.path());
        let digest = agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap());
        let config = format!(
            "schema_version=2\n[managed_attempt_binding]\nenabled=false\nrole_name='managed'\npool_server='pool'\npool_executable_sha256='{frontend}'\nbridge_hook_sha256='{hook}'\nhost_id='host'\npool_resident_executable='/usr/bin/false'\npool_resident_sha256='{pool}'\n[[managed_attempt_binding.bridge_custody_readers]]\nreader_id='custody'\nworker_uid={uid}\nexecutable='{executable}'\nexecutable_sha256='{digest}'\nrun_route={{component_store_id='{sid}',route_id='run',worker_uid={uid}}}\nbridge_route={{component_store_id='00000000-0000-4000-8000-000000000001',route_id='bridge',worker_uid={uid}}}\nproducer_scopes=['scope']\nproducer_scope_prefixes=['pool/store/pool/']\n",
            frontend = "a".repeat(64),
            hook = "c".repeat(64),
            pool = "b".repeat(64),
            executable = executable.display(),
        );
        std::fs::write(home.path().join("config.toml"), &config).unwrap();
        let writer = crate::writer_test_harness::Harness::start(home.path());
        let service = Service::new(home.path().into()).with_writer(writer.broker());
        let socket = dir.path().join("reader.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        crate::attempt_transport::test_peer::wait_build_age(&executable).await;
        let output = dir.path().join("reply");
        let digest = "d".repeat(64);
        for (method, scope, expected, expected_error) in [
            ("_owner_effect_lookup_v2", "scope", "absent", None),
            ("_owner_effect_abort_v2", "scope", "aborted", None),
            ("_owner_effect_lookup_v2", "scope", "aborted", None),
            (
                "_owner_effect_lookup_v2",
                "pool/store/pool/member",
                "absent",
                None,
            ),
            (
                "_owner_effect_abort_v2",
                "pool/store/pool/member/final-notice",
                "aborted",
                None,
            ),
            // Bridge compaction confirmation uses this same exact-key lookup.
            (
                "_owner_effect_lookup_v2",
                "pool/store/pool/member/final-notice",
                "aborted",
                None,
            ),
            (
                "_owner_effect_lookup_v2",
                "pool/store/pool/",
                "",
                Some("peer_not_authorized"),
            ),
            (
                "_owner_effect_abort_v2",
                "pool/store/pool/member//other",
                "",
                Some("peer_not_authorized"),
            ),
            (
                "_owner_effect_lookup_v2",
                "pool/store/poolish/member",
                "",
                Some("peer_not_authorized"),
            ),
            (
                "_owner_effect_abort_v2",
                "foreign",
                "",
                Some("peer_not_authorized"),
            ),
        ] {
            let frame = json!({"id":"request","method":method,"params":{"version":2,"expected_store_id":sid,"producer_scope":scope,"effect_id":"effect","effect_digest":digest}}).to_string();
            let mut child =
                crate::attempt_transport::test_peer::child_at(&executable, "client", &socket)
                    .env("RUN_R2_PEER_FRAME", frame)
                    .env("RUN_R2_PEER_OUTPUT", &output)
                    .spawn()
                    .unwrap();
            let (stream, _) =
                tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
            serve_bridge_receipt(stream, &service).await.unwrap();
            assert!(child.wait().await.unwrap().success());
            let reply: Value = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
            if let Some(code) = expected_error {
                assert_eq!(reply["error"]["code"], code);
                assert_eq!(reply["error"]["version"], 2);
            } else {
                assert_eq!(reply["result"]["state"], expected);
                assert_eq!(reply["result"]["version"], 2);
                assert_reply_keys(
                    &reply["result"],
                    &[
                        "version",
                        "store_id",
                        "producer_scope",
                        "effect_id",
                        "effect_digest",
                        "state",
                        "protocol",
                        "native_id",
                        "reason",
                    ],
                );
            }
        }
        // Bridge's post-retirement compaction confirms this original accepted
        // receipt using the very same prefix-authorized lookup route.
        let frame=json!({"id":"confirm","method":"_owner_effect_lookup_v2","params":{"version":2,"expected_store_id":sid,"producer_scope":"pool/store/pool/member","effect_id":"confirmed","effect_digest":digest}}).to_string();
        let mut child =
            crate::attempt_transport::test_peer::child_at(&executable, "client", &socket)
                .env("RUN_R2_PEER_FRAME", frame)
                .env("RUN_R2_PEER_OUTPUT", &output)
                .spawn()
                .unwrap();
        let (stream, _) =
            tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
        serve_bridge_receipt(stream, &service).await.unwrap();
        assert!(child.wait().await.unwrap().success());
        let reply: Value = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
        assert_eq!(reply["result"]["state"], "accepted");
        assert_eq!(reply["result"]["native_id"], "turn-id");
        std::fs::write(
            home.path().join("config.toml"),
            config.replace("producer_scope_prefixes=['pool/store/pool/']\n", ""),
        )
        .unwrap();
        let frame=json!({"id":"revoked","method":"_owner_effect_lookup_v2","params":{"version":2,"expected_store_id":sid,"producer_scope":"pool/store/pool/member","effect_id":"confirmed","effect_digest":digest}}).to_string();
        let mut child =
            crate::attempt_transport::test_peer::child_at(&executable, "client", &socket)
                .env("RUN_R2_PEER_FRAME", frame)
                .env("RUN_R2_PEER_OUTPUT", &output)
                .spawn()
                .unwrap();
        let (stream, _) =
            tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
        serve_bridge_receipt(stream, &service).await.unwrap();
        assert!(child.wait().await.unwrap().success());
        let reply: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(reply["error"]["code"], "peer_not_authorized");
    }
}
