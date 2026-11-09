//! Closed private R2 NDJSON operations. Not bound to the public API, MCP or
//! default broker endpoints; callers must configure and authenticate peers.
#![allow(dead_code)]
use crate::{
    Error, Result,
    attempt_binding::{BridgeReader, PeerGrant, PoolReader},
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
        Error::Validation(message) if message == "attempt mismatch" => ("attempt_mismatch", "none"),
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
        Error::Validation(message) if message == "namespace recovery incomplete" => {
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

/// Dispatch one already authenticated request. Each caller is tied to the
/// configured grant; request data never chooses a socket, executable or token.
pub(crate) async fn dispatch(
    service: &Service,
    grant: &PeerGrant,
    bridge: &dyn BridgeReader,
    pool: &dyn PoolReader,
    method: &str,
    params: Value,
) -> Result<Value> {
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
                    "attempt_consensus_v1",
                    "attempt_admission_v1",
                    "attempt_namespace_retirement_v1",
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
                } => {
                    version(n)?;
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
                    let receipt = service
                        .attempt_current_egress_v1(grant, bridge, req)
                        .await?;
                    Ok(
                        json!({"version":1,"store_id":expected_store_id,"state":"current","receipt":receipt}),
                    )
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
            Ok(
                json!({"version":1,"store_id":req.expected_store_id,"state":record.state,"admission":record.admission,"outcome_digest":record.outcome_digest}),
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
                return Err(invalid("attempt request invalid"));
            }
            let req: Abort = parse(params)?;
            version(req.version)?;
            if req.request_id != req.intent_id || req.pool_route != grant.pool_route {
                return Err(Error::Conflict);
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
        _ => Err(Error::Unsupported("attempt method unsupported".into())),
    }
}
fn wire_id(value: &str) -> Result<()> {
    agent_run_domain::attempt_binding::validate_id(value)
}

/// Serve one bounded frame after same-UID and executable/build authentication.
/// The caller owns endpoint lifecycle and must not pass request-selected pins.
pub(crate) async fn serve_one(
    stream: UnixStream,
    pin: PeerPin,
    service: &Service,
    grant: &PeerGrant,
    bridge: &dyn BridgeReader,
    pool: &dyn PoolReader,
) -> Result<()> {
    let stream = tokio::task::spawn_blocking(move || {
        verify_peer(&stream, &pin)?;
        Ok::<_, Error>(stream)
    })
    .await
    .map_err(|_| invalid("attempt peer unavailable"))??;
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
    let reply = match dispatch(
        service,
        grant,
        bridge,
        pool,
        &request.method,
        request.params,
    )
    .await
    {
        Ok(result) => json!({"id":request.id,"result":result}),
        Err(error) => json!({"id":request.id,"error":closed_error(&error,&request.method,&store)}),
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
                if !reader.producer_scopes.contains(&parsed.producer_scope) {
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

fn peer_not_authorized() -> Error {
    Error::Broker {
        message: "attempt peer refused".into(),
        broker_error_code: Some("peer_not_authorized".into()),
        broker_error_data: Some(json!({"recovery":"none"})),
    }
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

/// Serve one configured Pool resident on the separate R2 socket. Current
/// configuration supplies all peer identities; no request selects a route or
/// credential. Disabled configurations preserve only exact-key recovery.
pub async fn serve_pool_attempt(stream: UnixStream, service: &Service) -> Result<()> {
    use crate::attempt_transport::{AuthenticatedBridge, AuthenticatedPool, Endpoint};
    let (config, _) = service.current_provider_config()?;
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
    let stream = tokio::task::spawn_blocking(move || {
        verify_peer(&stream, &auth_pin)?;
        Ok::<_, Error>(stream)
    })
    .await
    .map_err(|_| invalid("attempt peer unavailable"))??;
    let store = service
        .read(|reader| agent_run_store::store_identity::read(&reader.conn))
        .await?;
    if transport.run_route.component_store_id != store.to_string() {
        return Err(Error::StoreIdentityMismatch);
    }
    let pool = AuthenticatedPool::configured(
        Endpoint::configured(transport.pool_socket.clone(), pool_pin)?,
        transport.pool_route.clone(),
    )?;
    let bridge = AuthenticatedBridge::from_pinned_file(
        Endpoint::configured(transport.bridge_socket.clone(), bridge_pin)?,
        transport.bridge_route.clone(),
        &transport.bridge_reader_credential_file,
        &transport.bridge_reader_credential_sha256,
    )?;
    let grant = PeerGrant {
        enabled: settings.enabled,
        pool_route: transport.pool_route.clone(),
        run_route: transport.run_route.clone(),
        bridge_route: transport.bridge_route.clone(),
        producer: transport.producer.clone(),
        host_id: settings.host_id.clone(),
    };
    serve_one(stream, peer_pin, service, &grant, &bridge, &pool).await
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
            assert_eq!(result["code"], code);
            assert_eq!(result["recovery"], recovery);
        }
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
            bridge_route: route("bridge", "bridge"),
            producer: "pool".into(),
            host_id: "host".into(),
        };
        let pin = PeerPin {
            uid,
            executable,
            sha256: digest,
        };
        serve_one(stream, pin, &service, &grant, &Fake, &Fake)
            .await
            .unwrap();
        assert!(child.wait().await.unwrap().success());
        let reply: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(reply["id"], "one");
        assert_eq!(reply["result"]["store_id"], sid.to_string());
        assert_eq!(reply["result"]["capabilities"], json!([]));
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
        std::fs::write(home.path().join("config.toml"), config).unwrap();
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
        drop(store);
        let executable = crate::attempt_transport::test_peer::copy_peer(dir.path());
        let digest = agent_run_platform::fs::sha256(&std::fs::read(&executable).unwrap());
        // SAFETY: geteuid is read-only and has no preconditions.
        let uid = unsafe { libc::geteuid() };
        let config = format!(
            "schema_version=2\n[managed_attempt_binding]\nenabled=false\nrole_name='managed'\npool_server='pool'\npool_executable_sha256='{frontend}'\nbridge_hook_sha256='{hook}'\nhost_id='host'\npool_resident_executable='/usr/bin/false'\npool_resident_sha256='{pool}'\n[[managed_attempt_binding.bridge_custody_readers]]\nreader_id='custody'\nworker_uid={uid}\nexecutable='{executable}'\nexecutable_sha256='{digest}'\nrun_route={{component_store_id='{sid}',route_id='run',worker_uid={uid}}}\nbridge_route={{component_store_id='00000000-0000-4000-8000-000000000001',route_id='bridge',worker_uid={uid}}}\nproducer_scopes=['scope']\n",
            frontend = "a".repeat(64),
            hook = "c".repeat(64),
            pool = "b".repeat(64),
            executable = executable.display(),
        );
        std::fs::write(home.path().join("config.toml"), config).unwrap();
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
            }
        }
    }
}
