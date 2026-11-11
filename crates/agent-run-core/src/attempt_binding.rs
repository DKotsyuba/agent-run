//! Broker-only R2 orchestration. No public socket or capability is enabled.
//! Reader implementations must authenticate their pinned peer and grant first.
#![allow(dead_code)]
use crate::{Result, broker_writer::BROKER_BUDGET, error::invalid, service::Service};
use agent_run_domain::{
    Error, StoreId,
    attempt_binding::{
        self as wire, AdmissionRequest, ConsumeReceipt, CurrentReceipt, EgressCurrent, Incarnation,
        IngressCurrent, Observation, Route,
    },
    domain::AgentId,
};
use agent_run_store::{
    attempt_binding::{
        AdmissionRecord, ConsensusCurrent, NamespaceRecord, NamespaceRequest, PoolSeal,
    },
    ops::{
        AbortAttemptAdmission, AdmitAttempt, BeginAttemptAdmission, BeginAttemptCurrent,
        BeginAttemptEgress, CompleteAttemptRetirement, FenceAttemptNamespace, FinishAttemptEgress,
        InvokeAttemptRetirement, ObserveAttemptPoolSeal, PrepareAttemptRetirement,
        RecordAttemptIngress, SettleAttemptAdmission,
    },
};
use std::{future::Future, pin::Pin};

pub(super) type PeerFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Resolved only after authenticating the Pool resident and both pinned routes.
#[derive(Clone)]
pub(crate) struct PeerGrant {
    pub enabled: bool,
    pub pool_route: Route,
    pub run_route: Route,
    pub bridge_route: Route,
    pub producer: String,
    pub host_id: String,
}
impl PeerGrant {
    fn validate(&self) -> Result<()> {
        self.pool_route.validate()?;
        self.run_route.validate()?;
        self.bridge_route.validate()?;
        wire::validate_id(&self.producer)?;
        wire::validate_id(&self.host_id)?;
        if self.pool_route.worker_uid != self.run_route.worker_uid
            || self.pool_route.worker_uid != self.bridge_route.worker_uid
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BridgeContext {
    pub version: u32,
    pub store_id: String,
    #[serde(rename = "bridge_context_ref")]
    pub context_ref: String,
    pub invocation_key: String,
    pub observation: Observation,
    pub issuer_epoch: String,
    pub validity: String,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BridgeConsume {
    pub version: u32,
    pub store_id: String,
    pub receipt: ConsumeReceipt,
    pub validity: String,
    pub aborted: bool,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BridgeIssuer {
    pub version: u32,
    pub store_id: String,
    pub issuer_epoch: String,
    pub capabilities: Vec<String>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BridgeRetired {
    pub version: u32,
    pub store_id: String,
    pub producer: String,
    pub issuer_epoch: String,
    pub retirement_key: String,
    pub retired_count: u64,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BridgeActor {
    pub version: u32,
    pub store_id: String,
    pub producer: String,
    pub incarnation: Incarnation,
    pub launch_id: String,
    pub state: String,
    pub actor: Option<wire::Actor>,
    pub issuer_epoch: Option<String>,
}

/// Bridge's authenticated classification for one exact original incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ActorRecoveryState {
    Current,
    AwaitingRootHook,
    RelaunchRequired,
    Unavailable,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BridgeActorRecovery {
    pub version: u32,
    pub store_id: String,
    pub producer: String,
    pub incarnation: Incarnation,
    pub launch_id: String,
    pub state: ActorRecoveryState,
}

/// A classification is a negative hint, never actor or receipt authority.
#[derive(Debug)]
pub(crate) struct ActorRecoveryRefusal {
    pub request_id: String,
    pub state: ActorRecoveryState,
    pub incarnation: Incarnation,
    pub launch_id: String,
}
#[derive(Debug)]
pub(crate) enum EgressOutcome {
    Current(Box<CurrentReceipt>),
    ActorRecovery(ActorRecoveryRefusal),
}

/// Implementations must authenticate the configured reader grant, same-UID
/// peer, executable/build and exact pinned route before returning a response.
pub(crate) trait BridgeReader: Send + Sync {
    fn context<'a>(&'a self, request: &'a IngressCurrent) -> PeerFuture<'a, BridgeContext>;
    fn consume<'a>(&'a self, request: &'a AdmissionRequest) -> PeerFuture<'a, BridgeConsume>;
    fn issuer<'a>(&'a self, request: &'a NamespaceRequest) -> PeerFuture<'a, BridgeIssuer>;
    fn retire<'a>(&'a self, request: &'a NamespaceRequest) -> PeerFuture<'a, BridgeRetired>;
    fn actor<'a>(
        &'a self,
        _route: &'a Route,
        _producer: &'a str,
        _incarnation: &'a Incarnation,
        _launch_id: &'a str,
    ) -> PeerFuture<'a, BridgeActor> {
        Box::pin(async { Err(Error::Unsupported("Bridge actor lookup unavailable".into())) })
    }
    fn actor_recovery<'a>(
        &'a self,
        _route: &'a Route,
        _producer: &'a str,
        _incarnation: &'a Incarnation,
        _launch_id: &'a str,
    ) -> PeerFuture<'a, BridgeActorRecovery> {
        Box::pin(async {
            Err(Error::Unsupported(
                "Bridge actor recovery unavailable".into(),
            ))
        })
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PoolOutcome {
    pub version: u32,
    pub store_id: String,
    pub intent_id: String,
    pub security_digest: String,
    pub state: String,
    pub outcome_digest: Option<String>,
}
/// Implementations authenticate the configured Pool reader grant and build.
pub(crate) trait PoolReader: Send + Sync {
    fn outcome<'a>(
        &'a self,
        route: &'a Route,
        intent: &'a str,
        digest: &'a str,
    ) -> PeerFuture<'a, PoolOutcome>;
    fn seal<'a>(&'a self, request: &'a NamespaceRequest) -> PeerFuture<'a, PoolSeal>;
}

fn inspect(observed: &Incarnation, grant: &PeerGrant) -> Result<Incarnation> {
    observed.validate()?;
    grant.validate()?;
    if observed.host_id != grant.host_id || observed.uid != grant.run_route.worker_uid {
        return Err(invalid("attempt mismatch"));
    }
    #[cfg(target_os = "linux")]
    {
        let leader = agent_run_platform::process::inspect(
            i32::try_from(observed.pid).map_err(|_| invalid("attempt mismatch"))?,
        )?;
        let kernel = agent_run_platform::process::kernel_incarnation(&leader)?;
        let inspected = Incarnation {
            host_id: grant.host_id.clone(),
            boot_id: kernel.boot_id,
            pid_namespace_inode: kernel.pid_namespace_inode,
            uid: kernel.uid,
            pid: kernel.pid,
            start_ticks: kernel.start_ticks,
        };
        if &inspected != observed {
            return Err(invalid("attempt mismatch"));
        }
        Ok(inspected)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(Error::Unsupported(
            "managed attempt binding requires Linux".into(),
        ))
    }
}

fn grant_ingress(grant: &PeerGrant, request: &IngressCurrent) -> Result<()> {
    grant.validate()?;
    request.validate()?;
    if request.pool_route != grant.pool_route
        || request.run_route != grant.run_route
        || request.bridge_route != grant.bridge_route
        || request.producer != grant.producer
    {
        return Err(invalid("route mismatch"));
    }
    Ok(())
}
fn grant_admission(grant: &PeerGrant, request: &AdmissionRequest) -> Result<()> {
    grant.validate()?;
    request.validate()?;
    if request.pool_route != grant.pool_route
        || request.binding.run_route != grant.run_route
        || request.binding.bridge_route != grant.bridge_route
        || request.consumed_ref.producer != grant.producer
    {
        return Err(invalid("route mismatch"));
    }
    Ok(())
}
fn grant_namespace(grant: &PeerGrant, request: &NamespaceRequest) -> Result<()> {
    grant.validate()?;
    request.validate()?;
    if request.pool_route != grant.pool_route
        || request.bridge_route != grant.bridge_route
        || request.producer != grant.producer
        || request.expected_store_id.to_string() != grant.run_route.component_store_id
    {
        return Err(invalid("route mismatch"));
    }
    Ok(())
}

impl Service {
    /// Request-boundary SHA reload of the explicit opt-in. Invalid revisions
    /// never grant new authority; recovery entry points use their pinned route.
    pub(super) fn attempt_enabled(&self, grant: &PeerGrant) -> Result<()> {
        grant.validate()?;
        let (config, _) = self.current_provider_config()?;
        if !grant.enabled
            || !config
                .managed_attempt_binding
                .as_ref()
                .is_some_and(|s| s.enabled && s.host_id == grant.host_id)
        {
            return Err(Error::Unsupported(
                "managed attempt binding disabled".into(),
            ));
        }
        Ok(())
    }

    /// Resolve the current local pin and compare its routes with the already
    /// authenticated connection before preparing or attesting enrollment.
    pub(crate) fn managed_enrollment_grant(
        &self,
        peer: &PeerGrant,
    ) -> Result<agent_run_store::managed_enrollment::Grant> {
        peer.validate()?;
        let (config, _) = self.current_provider_config()?;
        let grant = agent_run_store::managed_enrollment::Grant::from_config(&config)?;
        if grant.pool_route != peer.pool_route
            || grant.run_route != peer.run_route
            || grant.bridge_route != peer.bridge_route
            || grant.producer != peer.producer
            || grant.host_id != peer.host_id
        {
            return Err(invalid("enrollment transport changed"));
        }
        Ok(grant)
    }

    /// Resolve a member through its original Pool enrollment, then reuse the
    /// durable R2 egress observation for the same immutable lineage.
    pub(crate) async fn managed_enrollment_current(
        &self,
        grant: &PeerGrant,
        bridge: &dyn BridgeReader,
        key: agent_run_domain::enrollment::Key,
        member_id: String,
        current_request_id: String,
    ) -> Result<serde_json::Value> {
        grant.validate()?;
        key.validate(&grant.pool_route)?;
        wire::validate_id(&member_id)?;
        wire::validate_id(&current_request_id)?;
        let lookup_key = key.clone();
        let lookup_route = grant.pool_route.clone();
        let lookup_member = member_id.clone();
        let (admission, member, original_grant) = self
            .read(move |store| {
                store.managed_enrollment_member(&lookup_key, &lookup_route, &lookup_member)
            })
            .await?;
        if self.managed_enrollment_grant(grant)? != original_grant {
            return Err(invalid("enrollment transport changed"));
        }
        let request_id = format!(
            "p3c_{}",
            agent_run_domain::canonical::sha256_hex(
                &serde_json::json!([
                    grant.pool_route,
                    key.pool_store_id,
                    key.request_id,
                    member_id,
                    current_request_id
                ]),
                true
            )
        );
        let request = EgressCurrent {
            expected_store_id: key.expected_store_id.clone(),
            request_id,
            lineage_root_id: member.lineage_root_id.clone(),
            bound_execution_id: member.execution_id.clone(),
            pool_route: grant.pool_route.clone(),
            run_route: grant.run_route.clone(),
            bridge_route: grant.bridge_route.clone(),
            producer: grant.producer.clone(),
            host_id: grant.host_id.clone(),
        };
        // Only an authenticated, exact-launch Bridge report that the root hook
        // has not yet been captured is "not ready". Foreign or malformed
        // echoes, a lineage with no live candidate, relaunch/unavailable
        // recovery states and transport failures keep plain R2 egress codes.
        let receipt = match self
            .attempt_current_egress_with_recovery(grant, bridge, request, true)
            .await?
        {
            EgressOutcome::Current(receipt) => *receipt,
            EgressOutcome::ActorRecovery(refusal)
                if refusal.state == ActorRecoveryState::AwaitingRootHook =>
            {
                return Err(invalid("attempt not ready"));
            }
            EgressOutcome::ActorRecovery(_) => return Err(invalid("egress binding unavailable")),
        };
        // Bridge I/O is outside SQLite. A re-pin while that lookup is in flight
        // must not let the old transport's result escape as P3 authority.
        if self.managed_enrollment_grant(grant)? != original_grant {
            return Err(invalid("enrollment transport changed"));
        }
        if receipt.lineage_root_id != member.lineage_root_id
            || receipt.attempt.lineage_root_id != member.lineage_root_id
        {
            return Err(invalid("attempt mismatch"));
        }
        let lookup_receipt = receipt.clone();
        let qualification = self
            .read(move |store| store.managed_enrollment_qualification(&lookup_receipt))
            .await?;
        Ok(
            serde_json::json!({"version":1,"store_id":key.expected_store_id,"request_id":key.request_id,"member_id":member_id,"state":"current","admission_receipt_id":admission.receipt_id,"receipt":receipt,"qualification":qualification}),
        )
    }
    /// Query Bridge independently of ingress after freezing the current launch.
    pub(crate) async fn attempt_current_egress_v1(
        &self,
        grant: &PeerGrant,
        bridge: &dyn BridgeReader,
        request: EgressCurrent,
    ) -> Result<CurrentReceipt> {
        match self
            .attempt_current_egress_with_recovery(grant, bridge, request, false)
            .await?
        {
            EgressOutcome::Current(receipt) => Ok(*receipt),
            EgressOutcome::ActorRecovery(_) => Err(invalid("egress binding unavailable")),
        }
    }

    /// Opt-in recovery reports only a separately authenticated Bridge result.
    pub(crate) async fn attempt_current_egress_with_recovery(
        &self,
        grant: &PeerGrant,
        bridge: &dyn BridgeReader,
        request: EgressCurrent,
        recovery_enabled: bool,
    ) -> Result<EgressOutcome> {
        grant.validate()?;
        request.validate()?;
        if request.pool_route != grant.pool_route
            || request.run_route != grant.run_route
            || request.bridge_route != grant.bridge_route
            || request.producer != grant.producer
            || request.host_id != grant.host_id
        {
            return Err(invalid("route mismatch"));
        }
        let enabled = self.attempt_enabled(grant).is_ok();
        let begin = self
            .lane("attempt-current")?
            .local(
                BeginAttemptEgress {
                    request: request.clone(),
                    enabled,
                },
                BROKER_BUDGET,
            )
            .await?;
        if let Some(receipt) = begin.receipt {
            return Ok(EgressOutcome::Current(Box::new(receipt)));
        }
        let candidate = begin
            .candidate
            .ok_or_else(|| invalid("egress binding unavailable"))?;
        inspect(&candidate.incarnation, grant)?;
        let mut found = bridge
            .actor(
                &request.bridge_route,
                &request.producer,
                &candidate.incarnation,
                &candidate.attempt.launch_id,
            )
            .await?;
        let matches_candidate = |found: &BridgeActor| {
            found.version == 1
                && found.store_id == request.bridge_route.component_store_id
                && found.producer == request.producer
                && found.incarnation == candidate.incarnation
                && found.launch_id == candidate.attempt.launch_id
        };
        if !matches_candidate(&found)
            || !matches!(
                found.state.as_str(),
                "current" | "historical" | "unavailable"
            )
        {
            return Err(invalid("egress binding unavailable"));
        }
        if found.state != "current" {
            if !recovery_enabled {
                return Err(invalid("egress binding unavailable"));
            }
            let status = bridge
                .actor_recovery(
                    &request.bridge_route,
                    &request.producer,
                    &candidate.incarnation,
                    &candidate.attempt.launch_id,
                )
                .await?;
            if status.version != 1
                || status.store_id != request.bridge_route.component_store_id
                || status.producer != request.producer
                || status.incarnation != candidate.incarnation
                || status.launch_id != candidate.attempt.launch_id
            {
                return Err(invalid("egress binding unavailable"));
            }
            inspect(&candidate.incarnation, grant)?;
            if status.state != ActorRecoveryState::Current {
                return Ok(EgressOutcome::ActorRecovery(ActorRecoveryRefusal {
                    request_id: request.request_id,
                    state: status.state,
                    incarnation: status.incarnation,
                    launch_id: status.launch_id,
                }));
            }
            found = bridge
                .actor(
                    &request.bridge_route,
                    &request.producer,
                    &candidate.incarnation,
                    &candidate.attempt.launch_id,
                )
                .await?;
            if !matches_candidate(&found) {
                return Err(invalid("egress binding unavailable"));
            }
            if !matches!(
                found.state.as_str(),
                "current" | "historical" | "unavailable"
            ) {
                return Err(invalid("egress binding unavailable"));
            }
            if found.state != "current" {
                return Err(invalid("actor recovery in progress"));
            }
        }
        if found.version != 1
            || found.store_id != request.bridge_route.component_store_id
            || found.producer != request.producer
            || found.incarnation != candidate.incarnation
            || found.launch_id != candidate.attempt.launch_id
            || found.state != "current"
            || found
                .issuer_epoch
                .as_ref()
                .is_none_or(|id| wire::validate_id(id).is_err())
        {
            return Err(invalid("egress binding unavailable"));
        }
        let actor = found
            .actor
            .ok_or_else(|| invalid("egress binding unavailable"))?;
        actor.validate()?;
        let inspected = inspect(&candidate.incarnation, grant)?;
        let enabled = self.attempt_enabled(grant).is_ok();
        let receipt = self
            .lane("attempt-current")?
            .local(
                FinishAttemptEgress {
                    request,
                    candidate,
                    actor,
                    inspected,
                    enabled,
                },
                BROKER_BUDGET,
            )
            .await?;
        Ok(EgressOutcome::Current(Box::new(receipt)))
    }

    /// Use only the still-current original Bridge context for new ingress.
    pub(crate) async fn attempt_current_ingress_v1(
        &self,
        grant: &PeerGrant,
        bridge: &dyn BridgeReader,
        request: IngressCurrent,
    ) -> Result<CurrentReceipt> {
        grant_ingress(grant, &request)?;
        let store = request.expected_store_id.clone();
        let route = request.pool_route.clone();
        let request_id = request.request_id.clone();
        let candidate = self
            .read(move |reader| reader.attempt_current_by_request(&store, &route, &request_id))
            .await?;
        if let Some((fingerprint, receipt)) = candidate {
            if fingerprint != wire::current_fingerprint(&request)? {
                return Err(Error::Conflict);
            }
            return Ok(receipt);
        }
        self.attempt_enabled(grant)?;
        let context = bridge.context(&request).await?;
        if context.validity == "historical" {
            return Err(invalid("assertion historical"));
        }
        if context.version != 1
            || context.store_id != grant.bridge_route.component_store_id
            || context.context_ref != request.bridge_context_ref
            || context.invocation_key != request.invocation_key
            || context.validity != "current"
        {
            return Err(invalid("Bridge original context unavailable"));
        }
        wire::validate_id(&context.issuer_epoch)?;
        context.observation.validate()?;
        let inspected = inspect(&context.observation.incarnation, grant)?;
        let enabled = self.attempt_enabled(grant).is_ok();
        self.lane("attempt-current")?
            .local(
                BeginAttemptCurrent {
                    request: request.clone(),
                    launch_id: context.observation.launch_id.clone(),
                    enabled,
                },
                BROKER_BUDGET,
            )
            .await?;
        let enabled = self.attempt_enabled(grant).is_ok();
        self.lane("attempt-current")?
            .local(
                RecordAttemptIngress {
                    request,
                    observation: context.observation,
                    inspected,
                    enabled,
                },
                BROKER_BUDGET,
            )
            .await
    }

    /// Journal lookup intent first; fetch Bridge evidence before writer CAS.
    pub(crate) async fn attempt_admit_v1(
        &self,
        grant: &PeerGrant,
        bridge: &dyn BridgeReader,
        request: AdmissionRequest,
    ) -> Result<AdmissionRecord> {
        grant_admission(grant, &request)?;
        let enabled = self.attempt_enabled(grant).is_ok();
        self.lane("attempt-admission")?
            .local(
                BeginAttemptAdmission {
                    request: request.clone(),
                    enabled,
                },
                BROKER_BUDGET,
            )
            .await?;
        let store = request.expected_store_id.clone();
        let route = request.pool_route.clone();
        let id = request.intent_id.clone();
        let digest = request.security_digest.clone();
        if let Some(existing) = self
            .read(move |reader| reader.attempt_admission_lookup(&store, &route, &id, &digest))
            .await?
        {
            return Ok(existing);
        }
        let lookup = bridge.consume(&request).await?;
        if lookup.validity == "historical" || lookup.aborted {
            return Err(invalid("assertion historical"));
        }
        if lookup.version != 1
            || lookup.store_id != grant.bridge_route.component_store_id
            || lookup.validity != "current"
            || lookup.aborted
        {
            return Err(invalid("Bridge observation unavailable"));
        }
        let inspected = inspect(&lookup.receipt.observation.incarnation, grant)?;
        let enabled = self.attempt_enabled(grant).is_ok();
        self.lane("attempt-admission")?
            .local(
                AdmitAttempt {
                    request,
                    receipt: lookup.receipt,
                    inspected,
                    enabled,
                },
                BROKER_BUDGET,
            )
            .await
    }

    /// Fetch and verify the exclusive terminal Pool outcome before settlement.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn attempt_settle_v1(
        &self,
        grant: &PeerGrant,
        pool: &dyn PoolReader,
        store: StoreId,
        admission_id: String,
        intent_id: String,
        security_digest: String,
        outcome_digest: String,
    ) -> Result<AdmissionRecord> {
        grant.validate()?;
        let route = grant.pool_route.clone();
        let stored_store = store.clone();
        let stored_route = route.clone();
        let stored_intent = intent_id.clone();
        let stored_digest = security_digest.clone();
        if let Some(existing) = self
            .read(move |reader| {
                reader.attempt_admission_lookup(
                    &stored_store,
                    &stored_route,
                    &stored_intent,
                    &stored_digest,
                )
            })
            .await?
            && existing.state != "admitted"
        {
            if existing
                .admission
                .as_ref()
                .is_none_or(|a| a.admission_id != admission_id)
                || existing.outcome_digest.as_deref() != Some(outcome_digest.as_str())
            {
                return Err(Error::Conflict);
            }
            return Ok(existing);
        }
        let pool_result = pool.outcome(&route, &intent_id, &security_digest).await?;
        if pool_result.version != 1
            || pool_result.store_id != route.component_store_id
            || pool_result.intent_id != intent_id
            || pool_result.security_digest != security_digest
            || pool_result.outcome_digest.as_deref() != Some(outcome_digest.as_str())
            || wire::settlement_digest(
                &pool_result.store_id,
                &intent_id,
                &security_digest,
                &pool_result.state,
            )? != outcome_digest
        {
            return Err(Error::Conflict);
        }
        self.lane("attempt-admission")?
            .local(
                SettleAttemptAdmission {
                    store,
                    admission_id,
                    intent_id,
                    security_digest,
                    pool_route: route,
                    state: pool_result.state,
                    outcome_digest,
                },
                BROKER_BUDGET,
            )
            .await
    }

    /// Fence absent admission before any Pool I/O; existing admission needs a
    /// matching authenticated terminal Pool outcome.
    pub(crate) async fn attempt_abort_v1(
        &self,
        grant: &PeerGrant,
        pool: &dyn PoolReader,
        store: StoreId,
        intent_id: String,
        security_digest: String,
        outcome_digest: Option<String>,
    ) -> Result<AdmissionRecord> {
        grant.validate()?;
        let record = self
            .lane("attempt-admission")?
            .local(
                AbortAttemptAdmission {
                    store: store.clone(),
                    intent_id: intent_id.clone(),
                    security_digest: security_digest.clone(),
                    pool_route: grant.pool_route.clone(),
                    requested_outcome_digest: outcome_digest.clone(),
                },
                BROKER_BUDGET,
            )
            .await?;
        if record.state == "aborted_before_admit" {
            return Ok(record);
        }
        if record.state != "admitted" {
            if outcome_digest
                .as_ref()
                .is_some_and(|digest| record.outcome_digest.as_ref() != Some(digest))
            {
                return Err(Error::Conflict);
            }
            return Ok(record);
        }
        let Some(admission) = record.admission.as_ref() else {
            return Ok(record);
        };
        let digest = outcome_digest.ok_or_else(|| invalid("Pool outcome is required"))?;
        self.attempt_settle_v1(
            grant,
            pool,
            store,
            admission.admission_id.clone(),
            intent_id,
            security_digest,
            digest,
        )
        .await
    }

    /// Fence, wait for authenticated Pool sealing and local recovery, then
    /// retire at Bridge with the unchanged original key.
    pub(crate) async fn attempt_retire_namespace_v1(
        &self,
        grant: &PeerGrant,
        bridge: &dyn BridgeReader,
        pool: &dyn PoolReader,
        request: NamespaceRequest,
    ) -> Result<NamespaceRecord> {
        grant_namespace(grant, &request)?;
        let read_request = request.clone();
        if let Some(record) = self
            .read(move |reader| reader.attempt_retirement_lookup(&read_request))
            .await?
            && record.state == "retired"
        {
            return Ok(record);
        }
        // Disabled mode permits exact recovery reads and admission settlement,
        // but cannot begin or advance a new retirement operation.
        self.attempt_enabled(grant)?;
        self.lane("attempt-retirement")?
            .local(
                PrepareAttemptRetirement {
                    request: request.clone(),
                },
                BROKER_BUDGET,
            )
            .await?;
        let issuer = bridge.issuer(&request).await?;
        if issuer.version != 1
            || issuer.store_id != grant.bridge_route.component_store_id
            || !issuer
                .capabilities
                .iter()
                .any(|s| s == "namespace_retirement_v1")
        {
            return Err(Error::Conflict);
        }
        let fence = self
            .lane("attempt-retirement")?
            .local(
                FenceAttemptNamespace {
                    request: request.clone(),
                    current_issuer: issuer.issuer_epoch,
                },
                BROKER_BUDGET,
            )
            .await?;
        if fence.state == "retired" {
            return Ok(fence);
        }
        let seal = pool.seal(&request).await?;
        if !seal.matches_request(&request) {
            return Err(Error::Conflict);
        }
        self.lane("attempt-retirement")?
            .local(
                ObserveAttemptPoolSeal {
                    request: request.clone(),
                    seal: seal.clone(),
                },
                BROKER_BUDGET,
            )
            .await?;
        if seal.state == "draining" {
            return Err(invalid("namespace recovery in progress"));
        }
        let invoking = self
            .lane("attempt-retirement")?
            .local(
                InvokeAttemptRetirement {
                    request: request.clone(),
                    seal,
                },
                BROKER_BUDGET,
            )
            .await?;
        let receipt = bridge.retire(&request).await?;
        if receipt.version != 1
            || receipt.store_id != grant.bridge_route.component_store_id
            || receipt.producer != request.producer
            || receipt.issuer_epoch != request.issuer_epoch
            || receipt.retirement_key != request.retirement_key
        {
            return Err(Error::Conflict);
        }
        self.lane("attempt-retirement")?
            .local(
                CompleteAttemptRetirement {
                    request,
                    pool_seal_id: invoking.pool_seal_id.ok_or(Error::Conflict)?,
                    retired_count: receipt.retired_count,
                },
                BROKER_BUDGET,
            )
            .await
    }

    /// A denied process inspection is unknown, never a synthetic death verdict.
    pub(crate) async fn attempt_consensus_v1(
        &self,
        grant: &PeerGrant,
        store: StoreId,
        root: AgentId,
        bound: AgentId,
        expected: wire::AttemptRef,
        incarnation: Incarnation,
    ) -> Result<ConsensusCurrent> {
        grant.validate()?;
        self.attempt_enabled(grant)?;
        let inspected = inspect(&incarnation, grant).ok();
        let host = grant.host_id.clone();
        self.read(move |reader| {
            reader.attempt_consensus_current(
                &store,
                &root,
                &bound,
                &expected,
                &incarnation,
                inspected.as_ref(),
                &host,
            )
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FakeBridge {
        retire_calls: AtomicUsize,
    }
    impl BridgeReader for FakeBridge {
        fn context<'a>(&'a self, _: &'a IngressCurrent) -> PeerFuture<'a, BridgeContext> {
            Box::pin(async { Err(Error::Conflict) })
        }
        fn consume<'a>(&'a self, _: &'a AdmissionRequest) -> PeerFuture<'a, BridgeConsume> {
            Box::pin(async { Err(Error::Conflict) })
        }
        fn issuer<'a>(&'a self, request: &'a NamespaceRequest) -> PeerFuture<'a, BridgeIssuer> {
            Box::pin(async move {
                Ok(BridgeIssuer {
                    version: 1,
                    store_id: request.bridge_route.component_store_id.clone(),
                    issuer_epoch: "current".into(),
                    capabilities: vec!["namespace_retirement_v1".into()],
                })
            })
        }
        fn retire<'a>(&'a self, request: &'a NamespaceRequest) -> PeerFuture<'a, BridgeRetired> {
            Box::pin(async move {
                if self.retire_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(Error::Runtime("synthetic lost ACK".into()));
                }
                Ok(BridgeRetired {
                    version: 1,
                    store_id: request.bridge_route.component_store_id.clone(),
                    producer: request.producer.clone(),
                    issuer_epoch: request.issuer_epoch.clone(),
                    retirement_key: request.retirement_key.clone(),
                    retired_count: 4,
                })
            })
        }
    }
    struct FakePool {
        seal_calls: AtomicUsize,
    }
    impl PoolReader for FakePool {
        fn outcome<'a>(
            &'a self,
            _: &'a Route,
            _: &'a str,
            _: &'a str,
        ) -> PeerFuture<'a, PoolOutcome> {
            Box::pin(async { Err(Error::Conflict) })
        }
        fn seal<'a>(&'a self, request: &'a NamespaceRequest) -> PeerFuture<'a, PoolSeal> {
            Box::pin(async move {
                let first = self.seal_calls.fetch_add(1, Ordering::SeqCst) == 0;
                Ok(PoolSeal {
                    version: 1,
                    store_id: request.pool_route.component_store_id.clone(),
                    request_id: request.pool_seal_request_id.clone(),
                    seal_id: "seal".into(),
                    bridge_route: request.bridge_route.clone(),
                    producer: request.producer.clone(),
                    issuer_epoch: request.issuer_epoch.clone(),
                    retirement_key: request.retirement_key.clone(),
                    state: if first { "draining" } else { "sealed" }.into(),
                })
            })
        }
    }

    #[tokio::test]
    async fn bridge_namespace_retirement_drains_and_recovers_lost_ack() {
        let home = tempfile::tempdir().unwrap();
        let store = agent_run_store::fixture::initialize(home.path()).unwrap();
        let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
        drop(store);
        let config = format!(
            "schema_version=2\n[managed_attempt_binding]\nenabled=false\nrole_name='managed'\npool_server='pool'\npool_executable_sha256='{}'\nbridge_hook_sha256='{}'\nhost_id='host'\n",
            "a".repeat(64),
            "c".repeat(64)
        );
        std::fs::write(home.path().join("config.toml"), &config).unwrap();
        let writer = crate::writer_test_harness::Harness::start(home.path());
        let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
        // SAFETY: geteuid has no arguments or side effects.
        let uid = unsafe { libc::geteuid() };
        let route = |store: &str, name: &str| Route {
            component_store_id: store.into(),
            route_id: name.into(),
            worker_uid: uid,
        };
        let grant = PeerGrant {
            enabled: true,
            pool_route: route("pool-store", "pool"),
            run_route: route(&sid.to_string(), "run"),
            bridge_route: route("00000000-0000-4000-8000-000000000001", "bridge"),
            producer: "pool".into(),
            host_id: "host".into(),
        };
        let request = NamespaceRequest {
            expected_store_id: sid,
            request_id: "request".into(),
            pool_route: grant.pool_route.clone(),
            bridge_route: grant.bridge_route.clone(),
            producer: "pool".into(),
            issuer_epoch: "old".into(),
            retirement_key: "retire".into(),
            pool_seal_request_id: "seal-request".into(),
        };
        let bridge = FakeBridge {
            retire_calls: AtomicUsize::new(0),
        };
        let pool = FakePool {
            seal_calls: AtomicUsize::new(0),
        };
        assert!(matches!(
            service
                .attempt_retire_namespace_v1(&grant, &bridge, &pool, request.clone())
                .await,
            Err(Error::Unsupported(_))
        ));
        assert_eq!(bridge.retire_calls.load(Ordering::SeqCst), 0);
        assert_eq!(pool.seal_calls.load(Ordering::SeqCst), 0);
        std::fs::write(
            home.path().join("config.toml"),
            config.replace("enabled=false", "enabled=true"),
        )
        .unwrap();
        assert!(
            service
                .attempt_retire_namespace_v1(&grant, &bridge, &pool, request.clone())
                .await
                .is_err()
        );
        assert_eq!(bridge.retire_calls.load(Ordering::SeqCst), 0);
        assert!(
            service
                .attempt_retire_namespace_v1(&grant, &bridge, &pool, request.clone())
                .await
                .is_err()
        );
        assert_eq!(bridge.retire_calls.load(Ordering::SeqCst), 1);
        let done = service
            .attempt_retire_namespace_v1(&grant, &bridge, &pool, request.clone())
            .await
            .unwrap();
        assert_eq!(done.state, "retired");
        assert_eq!(done.retired_count, Some(4));
        assert_eq!(
            service
                .attempt_retire_namespace_v1(&grant, &bridge, &pool, request.clone())
                .await
                .unwrap(),
            done
        );
        assert_eq!(bridge.retire_calls.load(Ordering::SeqCst), 2);
        let mut params = serde_json::to_value(request).unwrap();
        params["version"] = json!(1);
        let reply = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &bridge,
            &pool,
            "_attempt_retire_namespace_v1",
            params,
        )
        .await
        .unwrap();
        crate::attempt_wire::assert_reply_keys(
            &reply,
            &[
                "version",
                "store_id",
                "state",
                "bridge_route",
                "producer",
                "issuer_epoch",
                "retirement_key",
                "retired_count",
            ],
        );
    }

    struct RecoveryBridge {
        actor: wire::Actor,
        state: ActorRecoveryState,
        actor_calls: AtomicUsize,
        recovery_calls: AtomicUsize,
        fail_actor: bool,
        fail_recovery: bool,
        mismatch: bool,
        current_on_retry: bool,
        foreign_on_retry: bool,
    }
    #[test]
    fn bridge_actor_recovery_dto_rejects_unknown_state_and_fields() {
        let value = json!({"version":1,"store_id":"00000000-0000-4000-8000-000000000001","producer":"pool","incarnation":{"host_id":"host","boot_id":"00000000-0000-4000-8000-000000000002","pid_namespace_inode":1,"uid":1000,"pid":2,"start_ticks":1},"launch_id":"00000000-0000-4000-8000-000000000003","state":"awaiting_root_hook"});
        assert!(serde_json::from_value::<BridgeActorRecovery>(value.clone()).is_ok());
        let mut invalid = value.clone();
        invalid["state"] = json!("future_unknown");
        assert!(serde_json::from_value::<BridgeActorRecovery>(invalid).is_err());
        let mut extra = value;
        extra["actor"] = json!({});
        assert!(serde_json::from_value::<BridgeActorRecovery>(extra).is_err());
    }
    impl BridgeReader for RecoveryBridge {
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
        fn actor<'a>(
            &'a self,
            route: &'a Route,
            producer: &'a str,
            incarnation: &'a Incarnation,
            launch_id: &'a str,
        ) -> PeerFuture<'a, BridgeActor> {
            Box::pin(async move {
                let count = self.actor_calls.fetch_add(1, Ordering::SeqCst);
                if self.fail_actor {
                    return Err(invalid("attempt peer unavailable"));
                }
                let current = self.current_on_retry && count > 0;
                Ok(BridgeActor {
                    version: 1,
                    store_id: route.component_store_id.clone(),
                    producer: producer.into(),
                    incarnation: incarnation.clone(),
                    launch_id: if self.foreign_on_retry && count > 0 {
                        uuid::Uuid::new_v4().to_string()
                    } else {
                        launch_id.into()
                    },
                    state: if current { "current" } else { "unavailable" }.into(),
                    actor: current.then(|| self.actor.clone()),
                    issuer_epoch: current.then(|| "issuer".into()),
                })
            })
        }
        fn actor_recovery<'a>(
            &'a self,
            route: &'a Route,
            producer: &'a str,
            incarnation: &'a Incarnation,
            launch_id: &'a str,
        ) -> PeerFuture<'a, BridgeActorRecovery> {
            Box::pin(async move {
                self.recovery_calls.fetch_add(1, Ordering::SeqCst);
                if self.fail_recovery {
                    return Err(invalid("attempt peer unavailable"));
                }
                Ok(BridgeActorRecovery {
                    version: 1,
                    store_id: route.component_store_id.clone(),
                    producer: producer.into(),
                    incarnation: incarnation.clone(),
                    launch_id: if self.mismatch {
                        uuid::Uuid::new_v4().to_string()
                    } else {
                        launch_id.into()
                    },
                    state: self.state,
                })
            })
        }
    }

    struct ObservationBridge {
        receipt: ConsumeReceipt,
        calls: AtomicUsize,
        context_calls: AtomicUsize,
    }

    struct DelayedActorBridge {
        actor: wire::Actor,
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }
    impl BridgeReader for DelayedActorBridge {
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
        fn actor<'a>(
            &'a self,
            route: &'a Route,
            producer: &'a str,
            incarnation: &'a Incarnation,
            launch_id: &'a str,
        ) -> PeerFuture<'a, BridgeActor> {
            Box::pin(async move {
                self.started.notify_one();
                self.release.notified().await;
                Ok(BridgeActor {
                    version: 1,
                    store_id: route.component_store_id.clone(),
                    producer: producer.into(),
                    incarnation: incarnation.clone(),
                    launch_id: launch_id.into(),
                    state: "current".into(),
                    actor: Some(self.actor.clone()),
                    issuer_epoch: Some("issuer".into()),
                })
            })
        }
    }
    impl BridgeReader for ObservationBridge {
        fn actor<'a>(
            &'a self,
            route: &'a Route,
            producer: &'a str,
            incarnation: &'a Incarnation,
            launch_id: &'a str,
        ) -> PeerFuture<'a, BridgeActor> {
            Box::pin(async move {
                Ok(BridgeActor {
                    version: 1,
                    store_id: route.component_store_id.clone(),
                    producer: producer.into(),
                    incarnation: incarnation.clone(),
                    launch_id: launch_id.into(),
                    state: "current".into(),
                    actor: Some(self.receipt.observation.actor.clone()),
                    issuer_epoch: Some(self.receipt.issuer_epoch.clone()),
                })
            })
        }
        fn context<'a>(&'a self, request: &'a IngressCurrent) -> PeerFuture<'a, BridgeContext> {
            Box::pin(async move {
                self.context_calls.fetch_add(1, Ordering::SeqCst);
                Ok(BridgeContext {
                    version: 1,
                    store_id: request.bridge_route.component_store_id.clone(),
                    context_ref: request.bridge_context_ref.clone(),
                    invocation_key: request.invocation_key.clone(),
                    observation: self.receipt.observation.clone(),
                    issuer_epoch: self.receipt.issuer_epoch.clone(),
                    validity: "current".into(),
                })
            })
        }
        fn consume<'a>(&'a self, request: &'a AdmissionRequest) -> PeerFuture<'a, BridgeConsume> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(BridgeConsume {
                    version: 1,
                    store_id: request.consumed_ref.bridge_route.component_store_id.clone(),
                    receipt: self.receipt.clone(),
                    validity: "current".into(),
                    aborted: false,
                })
            })
        }
        fn issuer<'a>(&'a self, _: &'a NamespaceRequest) -> PeerFuture<'a, BridgeIssuer> {
            Box::pin(async { Err(Error::Conflict) })
        }
        fn retire<'a>(&'a self, _: &'a NamespaceRequest) -> PeerFuture<'a, BridgeRetired> {
            Box::pin(async { Err(Error::Conflict) })
        }
    }
    struct OutcomePool;
    impl PoolReader for OutcomePool {
        fn outcome<'a>(
            &'a self,
            route: &'a Route,
            intent: &'a str,
            digest: &'a str,
        ) -> PeerFuture<'a, PoolOutcome> {
            Box::pin(async move {
                Ok(PoolOutcome {
                    version: 1,
                    store_id: route.component_store_id.clone(),
                    intent_id: intent.into(),
                    security_digest: digest.into(),
                    state: "committed".into(),
                    outcome_digest: Some(wire::settlement_digest(
                        &route.component_store_id,
                        intent,
                        digest,
                        "committed",
                    )?),
                })
            })
        }
        fn seal<'a>(&'a self, _: &'a NamespaceRequest) -> PeerFuture<'a, PoolSeal> {
            Box::pin(async { Err(Error::Conflict) })
        }
    }

    /// The first protected hook and the owner observation resolve the same
    /// externally enrolled root; neither may manufacture a missing actor.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn p3_rootless_first_hook_and_qualified_current() {
        use agent_run_domain::{
            attempt_binding::{Actor, Observation, RoleQualification},
            batch::{ExecutionRef, PinId},
            enrollment::{Admission, Key, Receipt},
        };
        let home = tempfile::tempdir().unwrap();
        let store = agent_run_store::fixture::initialize(home.path()).unwrap();
        let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
        let id = AgentId::new();
        let attempt = format!("att_{}", uuid::Uuid::new_v4().simple());
        let launch = uuid::Uuid::new_v4().to_string();
        let leader = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
        let k = agent_run_platform::process::kernel_incarnation(&leader).unwrap();
        let incarnation = Incarnation {
            host_id: "host".into(),
            boot_id: k.boot_id,
            pid_namespace_inode: k.pid_namespace_inode,
            uid: k.uid,
            pid: k.pid,
            start_ticks: k.start_ticks,
        };
        let identity = json!({"provider_identity_version":2,"provider_config":{"managed_attempt_binding":{"enabled":true,"role_name":"managed","host_id":"host"}},"authority":{"profile":"managed","harness":"codex","role_payload":{"config_revision":"b".repeat(64)}}});
        store.conn.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id,sequence,runtime_session_id,identity_json,pool_membership_ever) VALUES(?,'codex','model','managed','task','task','/fixture','{}','running',1,'cfg',?,1,'native',?,1)",rusqlite::params![id.as_str(),id.as_str(),identity.to_string()]).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,phase,ownership_active,process_identity,process_birth_time) VALUES(?,?,1,'running','{}',1,'spawning',1,?,?)",rusqlite::params![attempt,id.as_str(),leader.token,leader.birth]).unwrap();
        store
            .conn
            .execute(
                "INSERT INTO worker_capabilities(attempt_id,token_sha256,created_at) VALUES(?,?,1)",
                rusqlite::params![attempt, "f".repeat(64)],
            )
            .unwrap();
        store.conn.execute("INSERT INTO worker_lifecycle(attempt_id,phase,transition_at) VALUES(?,'running',1)",[&attempt]).unwrap();
        store.conn.execute("INSERT INTO attempt_launches(spawn_key,execution_id,sequence,attempt_id,launch_id,state,boot_id,pid_namespace_inode,uid,pid,start_ticks) VALUES(?,?,1,?,?,'bound',?,?,?,?,?)",rusqlite::params![attempt,id.as_str(),attempt,launch,incarnation.boot_id,incarnation.pid_namespace_inode,incarnation.uid,incarnation.pid,incarnation.start_ticks]).unwrap();
        let pool = Route {
            component_store_id: "00000000-0000-4000-8000-000000000011".into(),
            route_id: "pool".into(),
            worker_uid: incarnation.uid,
        };
        let run = Route {
            component_store_id: sid.to_string(),
            route_id: "run".into(),
            worker_uid: incarnation.uid,
        };
        let bridge_route = Route {
            component_store_id: "00000000-0000-4000-8000-000000000012".into(),
            route_id: "bridge".into(),
            worker_uid: incarnation.uid,
        };
        let grant = PeerGrant {
            enabled: true,
            pool_route: pool.clone(),
            run_route: run,
            bridge_route: bridge_route.clone(),
            producer: "pool".into(),
            host_id: "host".into(),
        };
        let key = Key {
            version: 1,
            expected_store_id: sid.clone(),
            pool_store_id: pool.component_store_id.clone(),
            request_id: "enroll".into(),
            fingerprint: "a".repeat(64),
        };
        let receipt = Receipt {
            version: 1,
            store_id: sid.clone(),
            request_id: key.request_id.clone(),
            receipt_id: uuid::Uuid::new_v4().to_string(),
            fingerprint: key.fingerprint.clone(),
            state: "admitted".into(),
            jobs: vec![Admission {
                index: 0,
                member_id: "member".into(),
                lineage_root_id: id.clone(),
                execution_id: id.clone(),
                sequence: 1,
                execution_ref: ExecutionRef::from_first_provider_attempt(&id, &attempt).unwrap(),
                pin_id: PinId::new(),
                managed_role_name: "managed".into(),
                role_digest: "b".repeat(64),
            }],
        };
        let route_json = serde_json::to_string(&pool).unwrap();
        let config = format!(
            "schema_version=2\n[managed_attempt_binding]\nenabled=true\nrole_name='managed'\npool_server='pool'\npool_executable_sha256='{}'\nbridge_hook_sha256='{}'\nhost_id='host'\npool_resident_executable='/usr/bin/false'\npool_resident_sha256='{}'\n[managed_attempt_binding.transport]\nrun_route={{component_store_id='{sid}',route_id='run',worker_uid={uid}}}\npool_route={{component_store_id='{}',route_id='pool',worker_uid={uid}}}\nbridge_route={{component_store_id='{}',route_id='bridge',worker_uid={uid}}}\nproducer='pool'\npool_socket='{}/pool.sock'\nbridge_socket='{}/bridge.sock'\nbridge_executable='/usr/bin/true'\nbridge_executable_sha256='{}'\nbridge_reader_credential_file='{}/reader'\nbridge_reader_credential_sha256='{}'\n",
            "a".repeat(64),
            "c".repeat(64),
            "b".repeat(64),
            pool.component_store_id,
            grant.bridge_route.component_store_id,
            home.path().display(),
            home.path().display(),
            "d".repeat(64),
            home.path().display(),
            "e".repeat(64),
            uid = incarnation.uid
        );
        std::fs::write(home.path().join("config.toml"), &config).unwrap();
        let pinned = agent_run_store::managed_enrollment::Grant::from_config(
            &agent_run_config::provider_config::ProviderConfig::load(home.path())
                .unwrap()
                .0,
        )
        .unwrap();
        store.conn.execute("INSERT INTO managed_enrollments(pool_route_json,pool_store_id,request_id,fingerprint_version,fingerprint,state,request_json,grant_json,receipt_id,receipt_json) VALUES(?,?,?,1,?,'admitted',NULL,?,?,?)",rusqlite::params![route_json,key.pool_store_id,key.request_id,key.fingerprint,serde_json::to_string(&pinned).unwrap(),receipt.receipt_id,serde_json::to_string(&receipt).unwrap()]).unwrap();
        store.conn.execute("INSERT INTO managed_enrollment_members(lineage_root_id,pool_route_json,pool_store_id,pool_id,request_id,member_id,member_index) VALUES(?,?,?,'pool',?,'member',0)",rusqlite::params![id.as_str(),route_json,key.pool_store_id,key.request_id]).unwrap();
        drop(store);
        let writer = crate::writer_test_harness::Harness::start(home.path());
        let service = Service::new(home.path().into()).with_writer(writer.broker());
        let actor = Actor {
            address: "host/codex/native".into(),
            role: "root".into(),
            native_id: "native".into(),
            epoch: 1,
        };
        let observation = Observation {
            incarnation: incarnation.clone(),
            launch_id: launch.clone(),
            actor: actor.clone(),
            native_call_id: "first-call".into(),
            tool_name: "mcp__pool__pool_get".into(),
            observed_at_ms: 1,
            expires_at_ms: 2,
        };
        let bridge = ObservationBridge {
            receipt: ConsumeReceipt {
                producer: "pool".into(),
                consume_request_id: "consume".into(),
                invocation_key: "first-hook".into(),
                security_digest: "a".repeat(64),
                observation,
                issuer_epoch: "issuer".into(),
                consumed_at_ms: 2,
            },
            calls: AtomicUsize::new(0),
            context_calls: AtomicUsize::new(0),
        };
        let current = || {
            service.managed_enrollment_current(
                &grant,
                &bridge,
                key.clone(),
                "member".into(),
                "observation-1".into(),
            )
        };
        assert!(
            matches!(current().await,Err(Error::Validation(ref message)) if message == "attempt not ready")
        );
        let store = agent_run_store::fixture::open(home.path()).unwrap();
        store
            .attempt_qualify_role(
                &id,
                &RoleQualification {
                    launch_id: launch.clone(),
                    attempt_id: attempt.clone(),
                    harness: "codex".into(),
                    role_digest: "b".repeat(64),
                    config_digest: "c".repeat(64),
                    inventory_digest: "d".repeat(64),
                    incarnation: incarnation.clone(),
                },
            )
            .unwrap();
        drop(store);
        let delayed = std::sync::Arc::new(DelayedActorBridge {
            actor: actor.clone(),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let pending = tokio::spawn({
            let service = service.clone();
            let grant = grant.clone();
            let key = key.clone();
            let delayed = delayed.clone();
            async move {
                service
                    .managed_enrollment_current(
                        &grant,
                        delayed.as_ref(),
                        key,
                        "member".into(),
                        "during-upgrade".into(),
                    )
                    .await
            }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            delayed.started.notified(),
        )
        .await
        .unwrap();
        std::fs::write(
            home.path().join("config.toml"),
            config
                .replace("producer='pool'", "producer='pool2'")
                .replace("bridge.sock", "bridge-new.sock"),
        )
        .unwrap();
        delayed.release.notify_one();
        assert!(
            matches!(pending.await.unwrap(),Err(Error::Validation(ref message)) if message == "enrollment transport changed")
        );
        std::fs::write(home.path().join("config.toml"), &config).unwrap();
        let unavailable = RecoveryBridge {
            actor: actor.clone(),
            state: ActorRecoveryState::AwaitingRootHook,
            actor_calls: AtomicUsize::new(0),
            recovery_calls: AtomicUsize::new(0),
            fail_actor: false,
            fail_recovery: false,
            mismatch: false,
            current_on_retry: false,
            foreign_on_retry: false,
        };
        assert!(
            matches!(service.managed_enrollment_current(&grant,&unavailable,key.clone(),"member".into(),"observation-1".into()).await,Err(Error::Validation(ref message)) if message == "attempt not ready")
        );
        // Only that exact awaiting-root-hook report is "not ready": transport
        // failure, relaunch-required and a foreign echo keep plain R2 codes.
        let refusal = |state, fail_actor, foreign_on_retry| RecoveryBridge {
            actor: actor.clone(),
            state,
            actor_calls: AtomicUsize::new(0),
            recovery_calls: AtomicUsize::new(0),
            fail_actor,
            fail_recovery: false,
            mismatch: false,
            current_on_retry: foreign_on_retry,
            foreign_on_retry,
        };
        for (peer, code, recovery) in [
            (
                refusal(ActorRecoveryState::AwaitingRootHook, true, false),
                "outcome_unknown",
                "same_key_lookup",
            ),
            (
                refusal(ActorRecoveryState::RelaunchRequired, false, false),
                "binding_unavailable",
                "operator",
            ),
            (
                refusal(ActorRecoveryState::Current, false, true),
                "binding_unavailable",
                "operator",
            ),
        ] {
            let error = service
                .managed_enrollment_current(
                    &grant,
                    &peer,
                    key.clone(),
                    "member".into(),
                    "observation-1".into(),
                )
                .await
                .unwrap_err();
            let reply = crate::attempt_wire::dispatch_error(
                &error.into(),
                "_managed_enrollment_current_v1",
                &sid.to_string(),
            );
            assert_eq!(
                (reply["code"].as_str(), reply["recovery"].as_str()),
                (Some(code), Some(recovery))
            );
        }
        let ingress = service
            .attempt_current_ingress_v1(
                &grant,
                &bridge,
                IngressCurrent {
                    expected_store_id: sid.clone(),
                    request_id: "first-ingress".into(),
                    bridge_route,
                    pool_route: pool.clone(),
                    run_route: grant.run_route.clone(),
                    producer: "pool".into(),
                    bridge_context_ref: "context".into(),
                    invocation_key: "first-hook".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(ingress.lineage_root_id, id);
        let result = current().await.unwrap();
        crate::attempt_wire::assert_reply_keys(
            &result,
            &[
                "version",
                "store_id",
                "request_id",
                "member_id",
                "state",
                "admission_receipt_id",
                "receipt",
                "qualification",
            ],
        );
        crate::attempt_wire::assert_reply_keys(
            &result["qualification"],
            &[
                "launch_id",
                "role_digest",
                "config_digest",
                "inventory_digest",
            ],
        );
        assert_eq!(result["receipt"]["lineage_root_id"], json!(id));
        assert_eq!(result["receipt"]["attempt"]["launch_id"], launch);
        assert_eq!(result["qualification"]["inventory_digest"], "d".repeat(64));
        let wire_result = crate::attempt_wire::dispatch(&service,&grant,&bridge,&OutcomePool,"_managed_enrollment_current_v1",json!({"version":1,"expected_store_id":sid,"pool_store_id":key.pool_store_id,"request_id":key.request_id,"fingerprint":key.fingerprint,"member_id":"member","current_request_id":"observation-1"})).await.unwrap();
        assert_eq!(wire_result, result);
        let replay = service
            .managed_enrollment_current(
                &grant,
                &unavailable,
                key.clone(),
                "member".into(),
                "observation-1".into(),
            )
            .await
            .unwrap();
        assert_eq!(replay, result);
        std::fs::write(
            home.path().join("config.toml"),
            config
                .replace("producer='pool'", "producer='pool2'")
                .replace("bridge.sock", "bridge-new.sock"),
        )
        .unwrap();
        let mut repinned = grant.clone();
        repinned.producer = "pool2".into();
        let old_current = crate::attempt_wire::dispatch(&service,&repinned,&bridge,&OutcomePool,"_managed_enrollment_current_v1",json!({"version":1,"expected_store_id":sid,"pool_store_id":key.pool_store_id,"request_id":key.request_id,"fingerprint":key.fingerprint,"member_id":"member","current_request_id":"after-upgrade"})).await.unwrap_err();
        assert_eq!(
            crate::attempt_wire::dispatch_error(
                &old_current,
                "_managed_enrollment_current_v1",
                &sid.to_string()
            )["code"],
            "recovery_required"
        );
        let recovery_key = json!({"version":1,"expected_store_id":sid,"pool_store_id":key.pool_store_id,"request_id":key.request_id,"fingerprint":key.fingerprint});
        let recovered = crate::attempt_wire::dispatch(
            &service,
            &repinned,
            &bridge,
            &OutcomePool,
            "_managed_start_lookup_v1",
            recovery_key.clone(),
        )
        .await
        .unwrap();
        assert_eq!(recovered["state"], "admitted");
        let aborted = crate::attempt_wire::dispatch(
            &service,
            &repinned,
            &bridge,
            &OutcomePool,
            "_managed_start_abort_v1",
            recovery_key,
        )
        .await
        .unwrap();
        assert_eq!(aborted["state"], "admitted");
        assert!(
            service
                .managed_enrollment_current(
                    &grant,
                    &bridge,
                    key,
                    "foreign".into(),
                    "observation-1".into()
                )
                .await
                .is_err()
        );
    }

    /// A synthetic enrolled Run home: managed role, pinned Pool frontend and
    /// Bridge hook, one enabled account and a configurable Pool tool subset.
    #[cfg(target_os = "linux")]
    struct P3Home {
        sid: agent_run_domain::StoreId,
        pool: Route,
        grant: PeerGrant,
        config: P3Config,
    }

    /// Renders the fixture configuration for `(enabled, allowed Pool tool)`.
    #[cfg(target_os = "linux")]
    type P3Config = Box<dyn Fn(bool, &str) -> String>;

    #[cfg(target_os = "linux")]
    fn p3_home(root: &std::path::Path) -> P3Home {
        use std::os::unix::fs::PermissionsExt;
        let root = root.to_path_buf();
        std::fs::create_dir_all(root.join("profiles")).unwrap();
        std::fs::create_dir(root.join("work")).unwrap();
        std::fs::write(root.join("profiles/managed.md"),"+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=['pool']\nrequired_constraints=[]\n+++\nManaged.\n").unwrap();
        let frontend = root.join("pool-frontend");
        let hook = root.join("bridge-hook");
        std::fs::copy("/usr/bin/true", &frontend).unwrap();
        std::fs::copy("/usr/bin/false", &hook).unwrap();
        std::fs::set_permissions(&frontend, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o700)).unwrap();
        let frontend_sha = agent_run_platform::fs::sha256(&std::fs::read(&frontend).unwrap());
        let hook_sha = agent_run_platform::fs::sha256(&std::fs::read(&hook).unwrap());
        let mut store = agent_run_store::fixture::initialize(&root).unwrap();
        let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
        store
            .register_account(&agent_run_domain::AccountRecord {
                account_id: "acct".parse().unwrap(),
                auth_family: "openai".parse().unwrap(),
                secret_ref: "native:codex".parse().unwrap(),
                status: agent_run_domain::AccountStatus::Enabled,
            })
            .unwrap();
        // SAFETY: geteuid is read-only.
        let uid = unsafe { libc::geteuid() };
        let pool = Route {
            component_store_id: "00000000-0000-4000-8000-000000000021".into(),
            route_id: "pool".into(),
            worker_uid: uid,
        };
        let run = Route {
            component_store_id: sid.to_string(),
            route_id: "run".into(),
            worker_uid: uid,
        };
        let bridge_route = Route {
            component_store_id: "00000000-0000-4000-8000-000000000022".into(),
            route_id: "bridge".into(),
            worker_uid: uid,
        };
        let grant = PeerGrant {
            enabled: true,
            pool_route: pool.clone(),
            run_route: run,
            bridge_route,
            producer: "pool".into(),
            host_id: "host".into(),
        };
        let (pool_store, bridge_store) = (
            pool.component_store_id.clone(),
            grant.bridge_route.component_store_id.clone(),
        );
        let run_store = sid.to_string();
        let config = move |enabled: bool, tool: &str| {
            format!(
                "schema_version=2\n[core]\nmax_active_agents=4\n[managed_attempt_binding]\nenabled={enabled}\nrole_name='managed'\npool_server='pool'\npool_executable_sha256='{frontend_sha}'\nbridge_hook_sha256='{hook_sha}'\nhost_id='host'\npool_resident_executable='/usr/bin/printf'\npool_resident_sha256='{}'\n[managed_attempt_binding.transport]\nrun_route={{component_store_id='{run_store}',route_id='run',worker_uid={uid}}}\npool_route={{component_store_id='{}',route_id='pool',worker_uid={uid}}}\nbridge_route={{component_store_id='{}',route_id='bridge',worker_uid={uid}}}\nproducer='pool'\npool_socket='{}/pool.sock'\nbridge_socket='{}/bridge.sock'\nbridge_executable='/usr/bin/cat'\nbridge_executable_sha256='{}'\nbridge_reader_credential_file='{}/reader'\nbridge_reader_credential_sha256='{}'\n[harnesses.codex]\nbinary='/usr/bin/true'\nhome='{}/codex'\nhooks=[{{event='PreToolUse',command=['{}'],matcher='mcp__pool__.*'}}]\n[mcp.pool]\ntransport='stdio'\ncommand='{}'\nargs=['mcp','--config','{}/pool.toml']\nenv_from=[]\napproval_mode='approve'\nallowed_tools=['{tool}']\n[providers.codex]\nharness='codex'\nconnection={{kind='native'}}\nauth_family='openai'\nlimits_source='none'\n[[providers.codex.models]]\nid='m'\n[[providers.codex.bindings]]\nlabel='test'\naccount='acct'\n",
                "b".repeat(64),
                pool_store,
                bridge_store,
                root.display(),
                root.display(),
                "c".repeat(64),
                root.display(),
                "d".repeat(64),
                root.display(),
                hook.display(),
                frontend.display(),
                root.display()
            ).replace("[mcp.pool]", &format!("[harnesses.claude-code]\nbinary='/usr/bin/true'\nhome='{}/claude'\n[mcp.pool]", root.display()))
        };
        drop(store);
        P3Home {
            sid,
            pool,
            grant,
            config: Box::new(config),
        }
    }

    #[cfg(target_os = "linux")]
    fn p3_request(
        root: &std::path::Path,
        sid: &agent_run_domain::StoreId,
        pool: &Route,
        id: &str,
    ) -> serde_json::Value {
        json!({"version":1,"expected_store_id":sid,"pool_store_id":pool.component_store_id,"pool_id":"pool","request_id":id,"mode":"start","jobs":[{"member_id":format!("member-{id}"),"selector":{"version":1,"role_name":"managed"},"spec":{"provider":"codex","model":"m","profile":"managed","task":"fixture","workdir":root.join("work"),"write":false,"explicit_finish":true}}],"preconditions":[]})
    }

    /// Exercise exact selector, real writer replay and a synthetic pre-spawn
    /// refusal; no native provider or Bridge service is started.
    #[cfg(all(target_os = "linux", feature = "test-fixtures"))]
    #[tokio::test]
    async fn p3_owner_only_exact_selector_and_single_enrollment() {
        let home = tempfile::tempdir().unwrap();
        let root = home.path().canonicalize().unwrap();
        std::fs::write(root.join("fixture-supervisor-spawn-error"), "").unwrap();
        let P3Home {
            sid,
            pool,
            grant,
            config,
        } = p3_home(&root);
        std::fs::write(root.join("config.toml"), config(false, "pool_get")).unwrap();
        agent_run_config::provider_config::ProviderConfig::load(&root).unwrap();
        let writer = crate::writer_test_harness::Harness::start(&root);
        let service = Service::new(root.clone()).with_writer(writer.broker());
        let bridge = FakeBridge {
            retire_calls: AtomicUsize::new(0),
        };
        let request = |id: &str| p3_request(&root, &sid, &pool, id);
        let error = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &bridge,
            &OutcomePool,
            "_managed_start_batch_v1",
            request("disabled"),
        )
        .await
        .unwrap_err();
        assert_eq!(
            crate::attempt_wire::dispatch_error(
                &error,
                "_managed_start_batch_v1",
                &sid.to_string()
            )["code"],
            "capability_missing"
        );
        std::fs::write(root.join("config.toml"), config(true, "pool_start_agent")).unwrap();
        let unsafe_request = request("unsafe");
        let error = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &bridge,
            &OutcomePool,
            "_managed_start_batch_v1",
            unsafe_request.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(
            crate::attempt_wire::dispatch_error(
                &error,
                "_managed_start_batch_v1",
                &sid.to_string()
            )["code"],
            "role_unqualified"
        );
        let digest = wire::enrollment_start_fingerprint(&unsafe_request).unwrap();
        let key = json!({"version":1,"expected_store_id":sid,"pool_store_id":pool.component_store_id,"request_id":"unsafe","fingerprint":digest});
        let error = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &bridge,
            &OutcomePool,
            "_managed_start_lookup_v1",
            key.clone(),
        )
        .await
        .unwrap_err();
        assert_eq!(
            crate::attempt_wire::dispatch_error(
                &error,
                "_managed_start_lookup_v1",
                &sid.to_string()
            )["code"],
            "request_in_progress"
        );
        let aborted = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &bridge,
            &OutcomePool,
            "_managed_start_abort_v1",
            key,
        )
        .await
        .unwrap();
        assert_eq!(aborted["state"], "aborted");
        crate::attempt_wire::assert_reply_keys(
            &aborted,
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
        std::fs::write(root.join("config.toml"), config(true, "pool_get")).unwrap();
        let good = request("good");
        let (first, second) = tokio::join!(
            crate::attempt_wire::dispatch(
                &service,
                &grant,
                &bridge,
                &OutcomePool,
                "_managed_start_batch_v1",
                good.clone()
            ),
            crate::attempt_wire::dispatch(
                &service,
                &grant,
                &bridge,
                &OutcomePool,
                "_managed_start_batch_v1",
                good.clone()
            )
        );
        let first = first.unwrap();
        crate::attempt_wire::assert_reply_keys(
            &first,
            &[
                "version",
                "store_id",
                "request_id",
                "receipt_id",
                "fingerprint",
                "state",
                "jobs",
            ],
        );
        crate::attempt_wire::assert_reply_keys(
            &first["jobs"][0],
            &[
                "index",
                "member_id",
                "lineage_root_id",
                "execution_id",
                "sequence",
                "execution_ref",
                "pin_id",
                "managed_role_name",
                "role_digest",
            ],
        );
        assert_eq!(first, second.unwrap());
        assert_eq!(first["state"], "admitted");
        assert_eq!(first["jobs"].as_array().unwrap().len(), 1);
        assert!(first["jobs"][0].get("attempt_id").is_none());
        let lookup = crate::attempt_wire::dispatch(&service,&grant,&bridge,&OutcomePool,"_managed_start_lookup_v1",json!({"version":1,"expected_store_id":sid,"pool_store_id":pool.component_store_id,"request_id":"good","fingerprint":first["fingerprint"]})).await.unwrap();
        crate::attempt_wire::assert_reply_keys(
            &lookup,
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
        crate::attempt_wire::assert_reply_keys(
            &lookup["jobs_state"][0],
            &[
                "index",
                "member_id",
                "state",
                "failure_code",
                "last_attempt",
                "incarnation",
                "qualification",
            ],
        );
        assert_eq!(lookup["receipt"], first);
        let stored = agent_run_store::fixture::open(&root).unwrap();
        assert_eq!(
            stored
                .conn
                .query_row("SELECT count(*) FROM agents", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            stored
                .conn
                .query_row("SELECT count(*) FROM history_pins", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            stored
                .conn
                .query_row("SELECT count(*) FROM managed_enrollment_members", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        drop(stored);
        std::fs::write(root.join("config.toml"), config(false, "pool_get")).unwrap();
        let replay = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &bridge,
            &OutcomePool,
            "_managed_start_batch_v1",
            good,
        )
        .await
        .unwrap();
        assert_eq!(replay, first);
        assert!(
            !agent_run_domain::tools::worker_registry()
                .iter()
                .any(|tool| tool.name.starts_with("_managed_start"))
        );
    }

    /// Panics inside launcher code, before any child could be observed.
    struct PanickingLauncher(std::path::PathBuf);
    impl crate::system_launch::SystemLauncher for PanickingLauncher {
        fn route(
            &self,
            provider: &agent_run_domain::ProviderId,
            harness: agent_run_domain::HarnessId,
        ) -> Result<crate::system_launch::SystemRoute> {
            Ok(crate::system_launch::SystemRoute {
                provider: provider.clone(),
                harness,
                state_home: self.0.clone(),
                private_home: self.0.clone(),
                operator_socket: self.0.join("operator.sock"),
                writer_socket: self.0.join("writer.sock"),
                worker_socket: self.0.join("worker.sock"),
                fingerprint: "a".repeat(64).parse().unwrap(),
            })
        }
        fn launch(
            &self,
            _: std::path::PathBuf,
            _: AgentId,
            _: crate::system_launch::SystemRoute,
            _: crate::broker_writer::BrokerLane,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
            panic!("synthetic enrollment handoff panic");
        }
    }

    /// A panicking supervisor handoff after a managed admission is recorded
    /// durably like an ordinary batch handoff; the receipt is still returned.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn p3_handoff_panic_records_supervisor_handoff_error() {
        let home = tempfile::tempdir().unwrap();
        let root = home.path().canonicalize().unwrap();
        let P3Home {
            sid,
            pool,
            grant,
            config,
        } = p3_home(&root);
        std::fs::write(root.join("config.toml"), config(true, "pool_get")).unwrap();
        let writer = crate::writer_test_harness::Harness::start(&root);
        let service = Service::new(root.clone())
            .with_writer(writer.broker())
            .with_system_launcher(std::sync::Arc::new(PanickingLauncher(root.clone())));
        let bridge = FakeBridge {
            retire_calls: AtomicUsize::new(0),
        };
        let receipt = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &bridge,
            &OutcomePool,
            "_managed_start_batch_v1",
            p3_request(&root, &sid, &pool, "panic"),
        )
        .await
        .unwrap();
        assert_eq!(receipt["state"], "admitted");
        let id = receipt["jobs"][0]["lineage_root_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let store = agent_run_store::fixture::open(&root).unwrap();
        let event: String = store
            .conn
            .query_row(
                "SELECT data_json FROM events WHERE agent_id=? AND kind='supervisor_handoff_error'",
                [id.as_str()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&event).unwrap()["reason"],
            "panic"
        );
    }

    struct DelayedBridge {
        receipt: ConsumeReceipt,
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }
    impl BridgeReader for DelayedBridge {
        fn context<'a>(&'a self, _: &'a IngressCurrent) -> PeerFuture<'a, BridgeContext> {
            Box::pin(async { Err(Error::Conflict) })
        }
        fn consume<'a>(&'a self, request: &'a AdmissionRequest) -> PeerFuture<'a, BridgeConsume> {
            Box::pin(async move {
                self.started.notify_one();
                self.release.notified().await;
                Ok(BridgeConsume {
                    version: 1,
                    store_id: request.consumed_ref.bridge_route.component_store_id.clone(),
                    receipt: self.receipt.clone(),
                    validity: "current".into(),
                    aborted: false,
                })
            })
        }
        fn issuer<'a>(&'a self, _: &'a NamespaceRequest) -> PeerFuture<'a, BridgeIssuer> {
            Box::pin(async { Err(Error::Conflict) })
        }
        fn retire<'a>(&'a self, _: &'a NamespaceRequest) -> PeerFuture<'a, BridgeRetired> {
            Box::pin(async { Err(Error::Conflict) })
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn synthetic_bridge_current_admit_replay_and_pool_settlement() {
        use agent_run_domain::attempt_binding::{
            Actor, AttemptRef, Binding, ConsumedRef, Observation, RoleQualification,
        };
        let home = tempfile::tempdir().unwrap();
        let store = agent_run_store::fixture::initialize(home.path()).unwrap();
        let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
        let id = AgentId::new();
        let attempt = format!("{}:1", id.as_str());
        let launch = uuid::Uuid::new_v4().to_string();
        let leader = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
        let k = agent_run_platform::process::kernel_incarnation(&leader).unwrap();
        let inc = Incarnation {
            host_id: "host".into(),
            boot_id: k.boot_id,
            pid_namespace_inode: k.pid_namespace_inode,
            uid: k.uid,
            pid: k.pid,
            start_ticks: k.start_ticks,
        };
        let identity = json!({"provider_identity_version":2,"provider_config":{"managed_attempt_binding":{"enabled":true,"role_name":"managed","host_id":"host"}},"authority":{"profile":"managed","harness":"codex","role_payload":{"config_revision":"b".repeat(64)}}});
        store.conn.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id,sequence,runtime_session_id,identity_json) VALUES(?,'codex','model','managed','task','task','/fixture','{}','running',1,'cfg',?,1,'native',?)",rusqlite::params![id.as_str(),id.as_str(),identity.to_string()]).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,phase,ownership_active,process_identity,process_birth_time) VALUES(?,?,1,'running','{}',1,'spawning',1,?,?)",rusqlite::params![attempt,id.as_str(),leader.token,leader.birth]).unwrap();
        store
            .conn
            .execute(
                "INSERT INTO worker_capabilities(attempt_id,token_sha256,created_at) VALUES(?,?,1)",
                rusqlite::params![attempt, "f".repeat(64)],
            )
            .unwrap();
        store.conn.execute("INSERT INTO worker_lifecycle(attempt_id,phase,transition_at) VALUES(?,'running',1)",[&attempt]).unwrap();
        store.conn.execute("INSERT INTO attempt_launches(spawn_key,execution_id,sequence,attempt_id,launch_id,state,boot_id,pid_namespace_inode,uid,pid,start_ticks) VALUES(?,?,1,?,?,'bound',?,?,?,?,?)",rusqlite::params![attempt,id.as_str(),attempt,launch,inc.boot_id,inc.pid_namespace_inode,inc.uid,inc.pid,inc.start_ticks]).unwrap();
        store
            .attempt_qualify_role(
                &id,
                &RoleQualification {
                    launch_id: launch.clone(),
                    attempt_id: attempt.clone(),
                    harness: "codex".into(),
                    role_digest: "b".repeat(64),
                    config_digest: "c".repeat(64),
                    inventory_digest: "d".repeat(64),
                    incarnation: inc.clone(),
                },
            )
            .unwrap();
        drop(store);
        let config = format!(
            "schema_version=2\n[managed_attempt_binding]\nenabled=true\nrole_name='managed'\npool_server='pool'\npool_executable_sha256='{}'\nbridge_hook_sha256='{}'\nhost_id='host'\n",
            "a".repeat(64),
            "c".repeat(64)
        );
        std::fs::write(home.path().join("config.toml"), config).unwrap();
        let writer = crate::writer_test_harness::Harness::start(home.path());
        let service = Service::new(home.path().into()).with_writer(writer.broker());
        let route = |store: &str, name: &str| Route {
            component_store_id: store.into(),
            route_id: name.into(),
            worker_uid: inc.uid,
        };
        let grant = PeerGrant {
            enabled: true,
            pool_route: route("pool-store", "pool"),
            run_route: route(&sid.to_string(), "run"),
            bridge_route: route("00000000-0000-4000-8000-000000000001", "bridge"),
            producer: "pool".into(),
            host_id: "host".into(),
        };
        let actor = Actor {
            address: "host/codex/native".into(),
            role: "root".into(),
            native_id: "native".into(),
            epoch: 1,
        };
        let source = ConsumedRef {
            bridge_route: grant.bridge_route.clone(),
            producer: "pool".into(),
            consume_request_id: "consume".into(),
            invocation_key: "invocation".into(),
            security_digest: "a".repeat(64),
            issuer_epoch: "issuer".into(),
        };
        let binding = Binding {
            pool_store_id: "pool-store".into(),
            pool_id: "pool".into(),
            member_id: "member".into(),
            binding_generation: 1,
            attempt: AttemptRef {
                run_store_id: sid.clone(),
                lineage_root_id: id.clone(),
                execution_id: id.clone(),
                sequence: 1,
                attempt_id: attempt,
                launch_id: launch.clone(),
            },
            actor: actor.clone(),
            run_route: grant.run_route.clone(),
            bridge_route: grant.bridge_route.clone(),
        };
        let observation = Observation {
            incarnation: inc.clone(),
            launch_id: launch,
            actor: actor.clone(),
            native_call_id: "call".into(),
            tool_name: "mcp__pool__pool_get".into(),
            observed_at_ms: 1,
            expires_at_ms: 2,
        };
        let bridge = ObservationBridge {
            receipt: ConsumeReceipt {
                producer: "pool".into(),
                consume_request_id: "consume".into(),
                invocation_key: "invocation".into(),
                security_digest: "a".repeat(64),
                observation,
                issuer_epoch: "issuer".into(),
                consumed_at_ms: 2,
            },
            calls: AtomicUsize::new(0),
            context_calls: AtomicUsize::new(0),
        };
        let make_recovery = |state| RecoveryBridge {
            actor: actor.clone(),
            state,
            actor_calls: AtomicUsize::new(0),
            recovery_calls: AtomicUsize::new(0),
            fail_actor: false,
            fail_recovery: false,
            mismatch: false,
            current_on_retry: false,
            foreign_on_retry: false,
        };
        let base = json!({"version":1,"expected_store_id":sid,"purpose":"egress","lineage_root_id":id,"bound_execution_id":id,"actor_recovery_version":1});
        for (version, code) in [
            (json!(null), "binding_unavailable"),
            (json!(2), "unsupported_version"),
        ] {
            let fake = make_recovery(ActorRecoveryState::AwaitingRootHook);
            let mut params = base.clone();
            params["request_id"] = json!("invalid-recovery-version");
            params["actor_recovery_version"] = version;
            let error = crate::attempt_wire::dispatch(
                &service,
                &grant,
                &fake,
                &OutcomePool,
                "_attempt_current_v1",
                params,
            )
            .await
            .unwrap_err();
            let response = crate::attempt_wire::dispatch_error(
                &error,
                "_attempt_current_v1",
                &sid.to_string(),
            );
            assert_eq!(response["code"], code);
            assert!(response.get("actor_recovery").is_none());
            assert_eq!(fake.actor_calls.load(Ordering::SeqCst), 0);
        }
        for (index, state, label, recovery) in [
            (
                0,
                ActorRecoveryState::AwaitingRootHook,
                "awaiting_root_hook",
                "same_key_lookup",
            ),
            (
                1,
                ActorRecoveryState::RelaunchRequired,
                "relaunch_required",
                "operator",
            ),
            (
                2,
                ActorRecoveryState::Unavailable,
                "unavailable",
                "operator",
            ),
        ] {
            let fake = make_recovery(state);
            let mut params = base.clone();
            params["request_id"] = json!(format!("actor-recovery-{index}"));
            let error = crate::attempt_wire::dispatch(
                &service,
                &grant,
                &fake,
                &OutcomePool,
                "_attempt_current_v1",
                params.clone(),
            )
            .await
            .unwrap_err();
            let response = crate::attempt_wire::dispatch_error(
                &error,
                "_attempt_current_v1",
                &sid.to_string(),
            );
            crate::attempt_wire::assert_reply_keys(
                &response,
                &[
                    "version",
                    "store_id",
                    "code",
                    "recovery",
                    "request_id",
                    "actor_recovery_version",
                    "actor_recovery",
                    "incarnation",
                    "launch_id",
                ],
            );
            assert_eq!(response["code"], "binding_unavailable");
            assert_eq!(response["actor_recovery"], label);
            assert_eq!(response["actor_recovery_version"], 1);
            assert_eq!(response["request_id"], params["request_id"]);
            assert_eq!(response["recovery"], recovery);
            assert_eq!(response["launch_id"], binding.attempt.launch_id);
            assert_eq!(response["incarnation"], json!(inc));
            assert_eq!(fake.actor_calls.load(Ordering::SeqCst), 1);
            assert_eq!(fake.recovery_calls.load(Ordering::SeqCst), 1);
        }
        // The original pending key can succeed after a fresh root hook, and its
        // committed receipt then replays even if Bridge is unavailable.
        let mut retry = base.clone();
        retry["request_id"] = json!("actor-recovery-0");
        let recovered = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &bridge,
            &OutcomePool,
            "_attempt_current_v1",
            retry.clone(),
        )
        .await
        .unwrap();
        assert_eq!(recovered["state"], "current");
        crate::attempt_wire::assert_reply_keys(
            &recovered,
            &["version", "store_id", "state", "receipt"],
        );
        let mut unavailable = make_recovery(ActorRecoveryState::Unavailable);
        unavailable.fail_actor = true;
        let replayed = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &unavailable,
            &OutcomePool,
            "_attempt_current_v1",
            retry,
        )
        .await
        .unwrap();
        assert_eq!(replayed, recovered);
        assert_eq!(unavailable.actor_calls.load(Ordering::SeqCst), 0);
        let mut fake = make_recovery(ActorRecoveryState::Current);
        fake.current_on_retry = true;
        let mut params = base.clone();
        params["request_id"] = json!("actor-recovery-raced-current");
        let current = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &fake,
            &OutcomePool,
            "_attempt_current_v1",
            params,
        )
        .await
        .unwrap();
        assert_eq!(current["receipt"]["actor"]["native_id"], "native");
        assert_eq!(fake.actor_calls.load(Ordering::SeqCst), 2);
        assert_eq!(fake.recovery_calls.load(Ordering::SeqCst), 1);
        for mode in [
            "actor-error",
            "status-error",
            "foreign-echo",
            "current-race",
            "foreign-on-retry",
            "legacy",
        ] {
            let mut fake = make_recovery(if matches!(mode, "current-race" | "foreign-on-retry") {
                ActorRecoveryState::Current
            } else {
                ActorRecoveryState::AwaitingRootHook
            });
            fake.fail_actor = mode == "actor-error";
            fake.fail_recovery = mode == "status-error";
            fake.mismatch = mode == "foreign-echo";
            fake.current_on_retry = mode == "foreign-on-retry";
            fake.foreign_on_retry = mode == "foreign-on-retry";
            let mut params = base.clone();
            params["request_id"] = json!(format!("actor-{mode}"));
            if mode == "legacy" {
                params
                    .as_object_mut()
                    .unwrap()
                    .remove("actor_recovery_version");
            }
            let error = crate::attempt_wire::dispatch(
                &service,
                &grant,
                &fake,
                &OutcomePool,
                "_attempt_current_v1",
                params,
            )
            .await
            .unwrap_err();
            let response = crate::attempt_wire::dispatch_error(
                &error,
                "_attempt_current_v1",
                &sid.to_string(),
            );
            assert!(response.get("actor_recovery").is_none(), "{mode}");
            if mode == "current-race" {
                assert_eq!(response["code"], "request_in_progress");
                assert_eq!(response["recovery"], "same_key_lookup");
            }
            if mode == "foreign-on-retry" {
                assert_eq!(response["code"], "binding_unavailable");
            }
            assert_eq!(
                fake.recovery_calls.load(Ordering::SeqCst),
                usize::from(!matches!(mode, "actor-error" | "legacy"))
            );
        }
        // Response negotiation does not alter the frozen egress fingerprint.
        let fake = make_recovery(ActorRecoveryState::AwaitingRootHook);
        let mut params = base.clone();
        params["request_id"] = json!("actor-legacy");
        let error = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &fake,
            &OutcomePool,
            "_attempt_current_v1",
            params,
        )
        .await
        .unwrap_err();
        let response =
            crate::attempt_wire::dispatch_error(&error, "_attempt_current_v1", &sid.to_string());
        assert_eq!(response["actor_recovery"], "awaiting_root_hook");
        // The Pool supplies only the original context reference and invocation;
        // the actor, process, launch and root all come from Run/Bridge evidence.
        let params = json!({"version":1,"expected_store_id":sid,"request_id":"wire-current","purpose":"ingress","observation_ref":"context","invocation_key":"invocation"});
        let wire_receipt = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &bridge,
            &OutcomePool,
            "_attempt_current_v1",
            params.clone(),
        )
        .await
        .unwrap();
        crate::attempt_wire::assert_reply_keys(
            &wire_receipt,
            &["version", "store_id", "state", "receipt"],
        );
        crate::attempt_wire::assert_reply_keys(
            &wire_receipt["receipt"],
            &[
                "receipt_id",
                "purpose",
                "lineage_root_id",
                "bound_execution_id",
                "attempt",
                "incarnation",
                "actor",
                "successor_proof",
            ],
        );
        crate::attempt_wire::assert_reply_keys(
            &wire_receipt["receipt"]["attempt"],
            &[
                "run_store_id",
                "lineage_root_id",
                "execution_id",
                "sequence",
                "attempt_id",
                "launch_id",
            ],
        );
        crate::attempt_wire::assert_reply_keys(
            &wire_receipt["receipt"]["incarnation"],
            &[
                "host_id",
                "boot_id",
                "pid_namespace_inode",
                "uid",
                "pid",
                "start_ticks",
            ],
        );
        crate::attempt_wire::assert_reply_keys(
            &wire_receipt["receipt"]["actor"],
            &["address", "role", "native_id", "epoch"],
        );
        crate::attempt_wire::assert_reply_keys(
            &wire_receipt["receipt"]["successor_proof"],
            &[
                "lineage_root_id",
                "bound_execution_id",
                "current_execution_id",
                "chain",
            ],
        );
        crate::attempt_wire::assert_reply_keys(
            &wire_receipt["receipt"]["successor_proof"]["chain"][0],
            &["execution_id", "parent_execution_id", "sequence"],
        );
        let by_id = crate::attempt_wire::dispatch(&service,&grant,&bridge,&OutcomePool,"_attempt_current_receipt_v1",json!({"version":1,"expected_store_id":sid,"receipt_id":wire_receipt["receipt"]["receipt_id"]})).await.unwrap();
        crate::attempt_wire::assert_reply_keys(&by_id, &["version", "store_id", "receipt"]);
        let consensus = crate::attempt_wire::dispatch(&service,&grant,&bridge,&OutcomePool,"_attempt_current_v1",json!({"version":1,"expected_store_id":sid,"request_id":"consensus-wire","purpose":"consensus","lineage_root_id":id,"bound_execution_id":id,"expected_attempt":binding.attempt,"expected_incarnation":inc})).await.unwrap();
        crate::attempt_wire::assert_reply_keys(
            &consensus,
            &[
                "version",
                "store_id",
                "state",
                "last_attempt",
                "last_incarnation",
                "successor_proof",
            ],
        );
        assert_eq!(wire_receipt["receipt"]["lineage_root_id"], id.as_str());
        assert_eq!(
            wire_receipt["receipt"]["attempt"]["launch_id"],
            binding.attempt.launch_id
        );
        assert_eq!(wire_receipt["receipt"]["actor"]["native_id"], "native");
        assert_eq!(
            wire_receipt["receipt"]["successor_proof"]["chain"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let mut forged = params.clone();
        forged["lineage_root_id"] = json!(id);
        assert!(
            crate::attempt_wire::dispatch(
                &service,
                &grant,
                &bridge,
                &OutcomePool,
                "_attempt_current_v1",
                forged
            )
            .await
            .is_err()
        );
        let egress = EgressCurrent {
            expected_store_id: sid.clone(),
            request_id: "egress".into(),
            lineage_root_id: id.clone(),
            bound_execution_id: id.clone(),
            bridge_route: grant.bridge_route.clone(),
            pool_route: grant.pool_route.clone(),
            run_route: grant.run_route.clone(),
            producer: grant.producer.clone(),
            host_id: grant.host_id.clone(),
        };
        let egress_receipt = service
            .attempt_current_egress_v1(&grant, &bridge, egress.clone())
            .await
            .unwrap();
        assert_eq!(egress_receipt.purpose, "egress");
        assert_eq!(egress_receipt.actor, actor);
        assert_eq!(
            service
                .attempt_current_egress_v1(&grant, &bridge, egress)
                .await
                .unwrap(),
            egress_receipt
        );
        let current = IngressCurrent {
            expected_store_id: sid.clone(),
            request_id: "current".into(),
            bridge_route: grant.bridge_route.clone(),
            pool_route: grant.pool_route.clone(),
            run_route: grant.run_route.clone(),
            producer: "pool".into(),
            bridge_context_ref: "context".into(),
            invocation_key: "invocation".into(),
        };
        let receipt = service
            .attempt_current_ingress_v1(&grant, &bridge, current.clone())
            .await
            .unwrap();
        assert_eq!(receipt.attempt, binding.attempt);
        assert_eq!(
            service
                .attempt_current_ingress_v1(&grant, &bridge, current.clone())
                .await
                .unwrap(),
            receipt
        );
        let request = AdmissionRequest {
            version: 1,
            expected_store_id: sid.clone(),
            request_id: "intent".into(),
            intent_id: "intent".into(),
            security_digest: "a".repeat(64),
            binding,
            consumed_ref: source,
            policy: "admission_snapshot".into(),
            pool_route: grant.pool_route.clone(),
        };
        let admitted = service
            .attempt_admit_v1(&grant, &bridge, request.clone())
            .await
            .unwrap();
        let mut admit_params = serde_json::to_value(&request).unwrap();
        admit_params.as_object_mut().unwrap().remove("pool_route");
        let wire_admit = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &bridge,
            &OutcomePool,
            "_attempt_admit_v1",
            admit_params.clone(),
        )
        .await
        .unwrap();
        crate::attempt_wire::assert_reply_keys(
            &wire_admit,
            &["version", "store_id", "state", "admission"],
        );
        crate::attempt_wire::assert_reply_keys(
            &wire_admit["admission"],
            &[
                "admission_id",
                "intent_id",
                "security_digest",
                "policy",
                "binding",
                "incarnation",
                "successor_proof",
                "consumed_ref",
            ],
        );
        assert_eq!(wire_admit["state"], "admitted");
        let lookup = crate::attempt_wire::dispatch(&service,&grant,&bridge,&OutcomePool,"_attempt_admission_lookup_v1",json!({"version":1,"expected_store_id":sid,"intent_id":request.intent_id,"security_digest":request.security_digest})).await.unwrap();
        crate::attempt_wire::assert_reply_keys(
            &lookup,
            &[
                "version",
                "store_id",
                "state",
                "admission",
                "outcome_digest",
            ],
        );
        let wire_abort = crate::attempt_wire::dispatch(&service,&grant,&bridge,&OutcomePool,"_attempt_admission_abort_v1",json!({"version":1,"expected_store_id":sid,"request_id":"shape-abort","intent_id":"shape-abort","security_digest":"f".repeat(64),"pool_route":grant.pool_route,"pool_outcome_digest":null})).await.unwrap();
        crate::attempt_wire::assert_reply_keys(
            &wire_abort,
            &[
                "version",
                "store_id",
                "state",
                "admission",
                "outcome_digest",
            ],
        );
        assert_eq!(wire_abort["state"], "aborted_before_admit");
        assert_eq!(
            service
                .attempt_admit_v1(&grant, &bridge, request.clone())
                .await
                .unwrap(),
            admitted
        );
        assert_eq!(bridge.calls.load(Ordering::SeqCst), 1);
        // A delayed Bridge response cannot win against a durable Run abort.
        let mut delayed = request.clone();
        delayed.intent_id = "delayed".into();
        delayed.request_id = "delayed".into();
        delayed.security_digest = "e".repeat(64);
        delayed.consumed_ref.security_digest = delayed.security_digest.clone();
        let mut delayed_receipt = bridge.receipt.clone();
        delayed_receipt.security_digest = delayed.security_digest.clone();
        let blocked = std::sync::Arc::new(DelayedBridge {
            receipt: delayed_receipt,
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let pending = tokio::spawn({
            let service = service.clone();
            let grant = grant.clone();
            let blocked = blocked.clone();
            let delayed = delayed.clone();
            async move {
                service
                    .attempt_admit_v1(&grant, blocked.as_ref(), delayed)
                    .await
            }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            blocked.started.notified(),
        )
        .await
        .unwrap();
        let aborted = service
            .attempt_abort_v1(
                &grant,
                &OutcomePool,
                sid.clone(),
                delayed.intent_id.clone(),
                delayed.security_digest.clone(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(aborted.state, "aborted_before_admit");
        blocked.release.notify_one();
        assert!(pending.await.unwrap().is_err());
        let mut pending_only = request.clone();
        pending_only.intent_id = "pending-only".into();
        pending_only.request_id = "pending-only".into();
        pending_only.security_digest = "9".repeat(64);
        pending_only.consumed_ref.security_digest = pending_only.security_digest.clone();
        let mut pending_receipt = bridge.receipt.clone();
        pending_receipt.security_digest = pending_only.security_digest.clone();
        let delayed = std::sync::Arc::new(DelayedBridge {
            receipt: pending_receipt,
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let pending = tokio::spawn({
            let service = service.clone();
            let grant = grant.clone();
            let delayed = delayed.clone();
            let request = pending_only.clone();
            async move {
                service
                    .attempt_admit_v1(&grant, delayed.as_ref(), request)
                    .await
            }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            delayed.started.notified(),
        )
        .await
        .unwrap();
        let config_path = home.path().join("config.toml");
        let config = std::fs::read_to_string(&config_path).unwrap();
        std::fs::write(
            &config_path,
            config.replace("enabled=true", "enabled=false"),
        )
        .unwrap();
        let before = bridge.context_calls.load(Ordering::SeqCst);
        assert_eq!(
            service
                .attempt_current_ingress_v1(&grant, &bridge, current.clone())
                .await
                .unwrap(),
            receipt
        );
        let mut disabled_current = current;
        disabled_current.request_id = "disabled-current-new".into();
        assert!(matches!(
            service
                .attempt_current_ingress_v1(&grant, &bridge, disabled_current)
                .await,
            Err(Error::Unsupported(_))
        ));
        assert_eq!(bridge.context_calls.load(Ordering::SeqCst), before);
        delayed.release.notify_one();
        assert!(matches!(pending.await.unwrap(), Err(Error::Unsupported(_))));
        assert!(matches!(
            service
                .attempt_admit_v1(&grant, &bridge, pending_only.clone())
                .await,
            Err(Error::Unsupported(_))
        ));
        assert_eq!(
            service
                .attempt_abort_v1(
                    &grant,
                    &OutcomePool,
                    sid.clone(),
                    pending_only.intent_id,
                    pending_only.security_digest,
                    None
                )
                .await
                .unwrap()
                .state,
            "aborted_before_admit"
        );
        assert_eq!(
            service
                .attempt_admit_v1(&grant, &bridge, request.clone())
                .await
                .unwrap(),
            admitted
        );
        let mut fresh = request.clone();
        fresh.intent_id = "fresh".into();
        fresh.request_id = "fresh".into();
        assert!(
            service
                .attempt_admit_v1(&grant, &bridge, fresh)
                .await
                .is_err()
        );
        assert_eq!(bridge.calls.load(Ordering::SeqCst), 1);
        let outcome = wire::settlement_digest(
            "pool-store",
            &request.intent_id,
            &request.security_digest,
            "committed",
        )
        .unwrap();
        let settled = service
            .attempt_settle_v1(
                &grant,
                &OutcomePool,
                sid.clone(),
                admitted.admission.clone().unwrap().admission_id,
                request.intent_id.clone(),
                request.security_digest.clone(),
                outcome.clone(),
            )
            .await
            .unwrap();
        assert_eq!(settled.state, "settled_committed");
        let settled_wire = crate::attempt_wire::dispatch(&service,&grant,&bridge,&OutcomePool,"_attempt_settle_v1",json!({"version":1,"expected_store_id":sid,"admission_id":admitted.admission.unwrap().admission_id,"intent_id":request.intent_id,"security_digest":request.security_digest,"pool_outcome_digest":outcome})).await.unwrap();
        crate::attempt_wire::assert_reply_keys(
            &settled_wire,
            &[
                "version",
                "store_id",
                "state",
                "admission",
                "outcome_digest",
            ],
        );
        let replay = crate::attempt_wire::dispatch(
            &service,
            &grant,
            &bridge,
            &OutcomePool,
            "_attempt_admit_v1",
            admit_params,
        )
        .await
        .unwrap();
        assert_eq!(
            replay, wire_admit,
            "admit replay remains the original admitted DTO after settlement"
        );
    }
}
