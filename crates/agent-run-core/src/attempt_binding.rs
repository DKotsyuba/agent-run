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
    /// Query Bridge independently of ingress after freezing the current launch.
    pub(crate) async fn attempt_current_egress_v1(
        &self,
        grant: &PeerGrant,
        bridge: &dyn BridgeReader,
        request: EgressCurrent,
    ) -> Result<CurrentReceipt> {
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
            return Ok(receipt);
        }
        let candidate = begin
            .candidate
            .ok_or_else(|| invalid("egress binding unavailable"))?;
        inspect(&candidate.incarnation, grant)?;
        let found = bridge
            .actor(
                &request.bridge_route,
                &request.producer,
                &candidate.incarnation,
                &candidate.attempt.launch_id,
            )
            .await?;
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
        self.lane("attempt-current")?
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
            .await
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
                .attempt_retire_namespace_v1(&grant, &bridge, &pool, request)
                .await
                .unwrap(),
            done
        );
        assert_eq!(bridge.retire_calls.load(Ordering::SeqCst), 2);
    }

    struct ObservationBridge {
        receipt: ConsumeReceipt,
        calls: AtomicUsize,
        context_calls: AtomicUsize,
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
                sid,
                admitted.admission.unwrap().admission_id,
                request.intent_id,
                request.security_digest,
                outcome,
            )
            .await
            .unwrap();
        assert_eq!(settled.state, "settled_committed");
    }
}
