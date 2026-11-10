//! Application facade: transport code has no direct access to adapters or SQL.
use crate::{
    Error, Result, adapters,
    broker_writer::{BrokerLane, BrokerWriter},
    config::Config,
    domain::{AgentId, OrchestratorRef, Outcome, StartRequest, Status, now},
    error::invalid,
    lifecycle::reconcile,
    logging,
    policy::{self, EffectivePolicy},
    process,
    profiles::{self, Profile},
    state::{Record, Store},
    supervisor, verify,
};
use agent_run_config::provider_config::ProviderConfig;
use agent_run_config::role_plan;
use agent_run_domain::{
    ProviderStartRequest, Sha256Digest,
    catalog::{AccountStatus, QuotaAdmissionError, QuotaCandidateSet, ResolvedLaunchAuthority},
    pool::{
        PoolDenial, PoolId, PoolPost, PoolQuery, PoolReplace, PoolSeat, PoolStartRequest,
        compose_member_task,
    },
};
use agent_run_store::{
    pool_admission::{PoolAdmission, PoolMemberAdmission},
    pool_replace::PoolReplacement,
    provider_admission::AdmissionInputs,
    writer::StoreReader,
};

/// Busy allowance of a broker-side read-only store job: reads never wait out a
/// writer's long transaction on an executor thread.
pub(crate) const READ_ALLOWANCE: Duration = Duration::from_millis(250);

/// Most `selection_stale` recalculations after the initial selection in
/// [`Service::admit_provider`]: four admission submissions in total.
pub const PROVIDER_STALE_RETRIES: u32 = 3;

/// Open pools the maintenance sweep inspects per pass.
const POOL_SWEEP_LIMIT: usize = 20;

/// Builds canonical and legacy-alias fingerprints for a validated request.
/// The compatibility fingerprint changes only its known orchestrator transport.
fn start_replay_fingerprints(request: &StartRequest) -> Result<Vec<String>> {
    let mut fingerprints = vec![agent_run_domain::canonical::sha256_hex(
        &serde_json::to_value(request)?,
        true,
    )];
    if let Some(reference) = &request.orchestrator {
        let alias = reference.legacy_transport()?;
        let mut legacy = serde_json::to_value(request)?;
        legacy["orchestrator"]["transport"] = json!(alias);
        fingerprints.push(agent_run_domain::canonical::sha256_hex(&legacy, true));
    }
    Ok(fingerprints)
}

use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::str::FromStr;
use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchIdentity {
    pub rust_identity_version: u32,
    #[serde(default)]
    pub replay_request_sha256: Option<String>,
    pub config: Config,
    pub profile: Profile,
    pub effective_policy: EffectivePolicy,
    pub runtime_home: Option<PathBuf>,
    pub snapshot_sha256: Option<String>,
}

/// Durable v2 admission authority; account credentials stay on the attempt.
///
/// The zero asset digest and absent runtime home mark preparation before the
/// supervisor seals assets. No launch may use this pending identity as proof.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderLaunchIdentity {
    /// Distinguishes new provider rows from historical runtime requests.
    pub provider_identity_version: u32,
    /// Exact hash of the strict provider request for replay.
    pub replay_request_sha256: String,
    /// Original explicit provider/model/account-label intent.
    pub provider_request: ProviderStartRequest,
    /// SHA-256 of exact config.toml bytes accepted at the request boundary.
    pub provider_config_sha256: String,
    /// Complete validated, credential-free launch configuration at admission.
    pub provider_config: ProviderConfig,
    /// Secret-free digest and ids of the operative normalized v2 config.
    pub provider_config_snapshot: Value,
    /// Frozen provider/model/role/scope; asset digest seals before spawning.
    pub authority: ResolvedLaunchAuthority,
    /// Root-approved system route, absent for ordinary per-user executions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_route: Option<crate::system_launch::SystemRoute>,
    /// Per-lineage generated home once sealed.
    pub runtime_home: Option<PathBuf>,
    /// Runtime asset index digest once sealed.
    pub snapshot_sha256: Option<String>,
}

impl ProviderLaunchIdentity {
    /// Reads an explicit v2 identity, verifies its original immutable raw
    /// configuration/replay proofs, then projects retired clocks out in memory.
    /// Remaining grants and normalized settings remain exact; historical bytes
    /// are never rewritten. Verifies the provider
    /// request against the staged projection and replay digest. An omitted
    /// effort may resolve to the frozen model default in newer admissions;
    /// older rows whose effective effort stayed absent remain readable. The stored
    /// request's Fast value is effective launch policy; the raw request remains
    /// unchanged for replay, independently of the captured global Fast override.
    pub fn read(row: &Record) -> Result<Self> {
        let mut document = row
            .identity
            .clone()
            .ok_or_else(|| invalid("provider launch identity is missing"))?;
        let original_request = document["provider_request"].clone();
        let original_config = document["provider_config"].clone();
        let original_snapshot = document["provider_config_snapshot"].clone();
        if document["replay_request_sha256"].as_str()
            != Some(agent_run_domain::canonical::sha256_hex(&original_request, true).as_str())
            || ProviderConfig::snapshot_document(&original_config)? != original_snapshot
        {
            return Err(Error::Integrity(
                "stored provider snapshot or replay proof changed".into(),
            ));
        }
        let projected_request = ProviderStartRequest::from_history(original_request)?;
        let projected_config = crate::config::historical_config(original_config);
        document["provider_request"] = serde_json::to_value(&projected_request)?;
        document["provider_config"] = projected_config.clone();
        let mut identity: Self = serde_json::from_value(document)
            .map_err(|_| invalid("provider launch identity is malformed"))?;
        // All nonretired fields must still have the exact normalized form that
        // the original snapshot sealed; removing clocks cannot normalize grants.
        if serde_json::to_value(&identity.provider_config)? != projected_config {
            return Err(Error::Integrity(
                "stored provider configuration is not normalized".into(),
            ));
        }
        identity.provider_config_snapshot = identity.provider_config.snapshot()?;
        identity.replay_request_sha256 = agent_run_domain::canonical::sha256_hex(
            &serde_json::to_value(&identity.provider_request)?,
            true,
        );
        let configured_default = identity
            .provider_config
            .providers
            .get(&identity.provider_request.provider)
            .and_then(|provider| {
                provider
                    .models
                    .iter()
                    .find(|model| model.id == identity.provider_request.model)
            })
            .and_then(|model| model.params.get("effort"));
        let effort_matches = identity.authority.effort == row.request.effort
            && (identity.provider_request.effort == row.request.effort
                || (identity.provider_request.effort.is_none()
                    && configured_default == row.request.effort.as_ref()));
        if identity.provider_identity_version != 2
            || identity.provider_request.provider.as_str() != row.request.runtime
            || identity.provider_request.model != row.request.model
            || identity.provider_request.profile != row.request.profile
            || identity.provider_request.task != row.request.task
            || identity.provider_request.workdir != row.request.workdir
            || identity.provider_request.explicit_finish != row.request.explicit_finish
            || !effort_matches
            || identity.provider_request.request_id != row.request.request_id
            || identity.provider_request.orchestrator != row.request.orchestrator
            || identity
                .provider_request
                .account
                .as_ref()
                .map(|label| label.as_str())
                != row.request.account.as_deref()
            || identity.authority.provider != identity.provider_request.provider
            || identity.authority.model != identity.provider_request.model
            || identity.authority.profile != identity.provider_request.profile
            || identity.authority.workdir != identity.provider_request.workdir
            || identity.provider_config.schema_version != 2
        {
            return Err(Error::Integrity(
                "stored provider authority contradicts request".into(),
            ));
        }
        if let Some(route) = &identity.system_route {
            route
                .validate_for(&identity.authority.provider, identity.authority.harness)
                .map_err(|_| {
                    Error::Integrity("stored system route contradicts provider authority".into())
                })?;
        }
        Ok(identity)
    }
}
impl LaunchIdentity {
    /// Reads immutable historical grants and projects only retired execution
    /// controls out of the embedded configuration. Runtime snapshot bytes and
    /// their digests remain untouched; malformed identities stay unsupported.
    pub fn read(row: &Record) -> Result<Self> {
        let mut value = row
            .identity
            .clone()
            .ok_or_else(|| invalid("native resume requires a recorded launch identity"))?;
        value["config"] = crate::config::historical_config(value["config"].clone());
        let result:Self=serde_json::from_value(value).map_err(|_|Error::Unsupported("resuming Python-created runs is not yet supported; their history and answers remain readable".into()))?;
        if result.rust_identity_version != 1 {
            return Err(invalid("unsupported launch identity version"));
        }
        if result.profile.name != row.request.profile
            || result.profile.write != row.request.write
            || result.profile.read_roots != row.request.read_roots
        {
            return Err(Error::Integrity(
                "stored role grants contradict the launch request".into(),
            ));
        }
        Ok(result)
    }
}
/// Transport-neutral application facade with one shared configuration cache.
#[derive(Clone)]
pub struct Service {
    /// Private agent-run home containing configuration and durable state.
    pub home: PathBuf,
    /// Last valid configuration keyed by its exact on-disk SHA-256 revision.
    config: Arc<RwLock<Option<CachedConfig>>>,
    /// Producer lanes of the resident writer; `None` until the broker attaches
    /// its writer (a service without one cannot mutate durable state).
    writer: Option<BrokerWriter>,
    /// Optional root-coordinated launcher; ordinary per-user services keep `None`.
    system_launcher: Option<Arc<dyn crate::system_launch::SystemLauncher>>,
    /// Bounded empty-read episodes shared by service clones; no credential data.
    pub(crate) pool_reads: Arc<std::sync::Mutex<crate::dispatch::PoolReadGuards>>,
}
/// One immutable configuration revision retained between service calls.
#[derive(Clone)]
struct CachedConfig {
    /// Lowercase SHA-256 of the exact `config.toml` bytes.
    revision: String,
    /// Parsed and validated configuration for `revision`.
    value: CachedConfigValue,
}
/// Exactly one validated schema for the currently active file revision.
#[derive(Clone)]
enum CachedConfigValue {
    /// Historical consumer configuration.
    Legacy(Config),
    /// Provider-oriented schema v2 configuration.
    Providers(ProviderConfig),
}
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Query {
    pub active: bool,
    pub orchestrator: Option<OrchestratorRef>,
    pub offset: usize,
    #[serde(default = "default_limit")]
    pub limit: usize,
    pub after_revision: Option<i64>,
    /// Committed transcript watermark to wake on journal-only progress.
    /// Journal rows never advance `after_revision`'s event revision, so an
    /// observer that must also see transcript and tool-count changes passes
    /// the page's `message_revision` here. Omission keeps the historical
    /// event-only wake behavior.
    pub after_message_revision: Option<i64>,
    pub wait_seconds: f64,
}
fn default_limit() -> usize {
    100
}
impl Default for Query {
    fn default() -> Self {
        Self {
            active: false,
            orchestrator: None,
            offset: 0,
            limit: 100,
            after_revision: None,
            after_message_revision: None,
            wait_seconds: 0.0,
        }
    }
}
impl Query {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0
            || self.limit > 1000
            || self.offset > i64::MAX as usize
            || !self.wait_seconds.is_finite()
            || !(0.0..=60.0).contains(&self.wait_seconds)
            || self.after_revision.is_some_and(|n| n < 0)
            || self.after_message_revision.is_some_and(|n| n < 0)
        {
            return Err(invalid("invalid agent page or wait arguments"));
        }
        if let Some(o) = &self.orchestrator {
            o.validate()?;
        }
        Ok(())
    }
}
impl Service {
    /// Block new admissions and durably capture the agents in a service restart.
    pub async fn begin_service_restart(&self, request_id: &str) -> Result<i64> {
        // Resolve the boot identity before entering the sole writer.
        agent_run_platform::monotonic::boot_id()?;
        self.lane("service-restart")?
            .local(
                agent_run_store::ops::BeginServiceRestart {
                    request_id: request_id.into(),
                    at: now(),
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await
    }

    /// Queue restart warnings or cancellation when their intervals have elapsed.
    pub(crate) async fn advance_service_restart(&self, restart_id: i64) -> Result<usize> {
        let (boot_id, now_ns) = agent_run_platform::monotonic::now()?;
        self.lane("service-restart")?
            .local(
                agent_run_store::ops::AdvanceServiceRestart {
                    restart_id,
                    boot_id,
                    now_ns,
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await
    }

    /// Observe a new broker writer and enter recovery for a prepared restart.
    pub async fn recover_service_restart(&self) -> Result<Option<i64>> {
        agent_run_platform::monotonic::boot_id()?;
        self.lane("service-restart")?
            .local(
                agent_run_store::ops::RecoverServiceRestart {
                    failed_restart: None,
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await
    }

    /// Continue a prepared restart on the same writer after the manager fails.
    pub async fn recover_failed_service_restart(&self, restart_id: i64) -> Result<Option<i64>> {
        self.lane("service-restart")?
            .local(
                agent_run_store::ops::RecoverServiceRestart {
                    failed_restart: Some(restart_id),
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await
    }

    /// Claim the one service-manager invocation after the workers stop.
    pub async fn claim_service_restart(&self, restart_id: i64) -> Result<bool> {
        self.lane("service-restart")?
            .local(
                agent_run_store::ops::ClaimServiceRestart { restart_id },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await
    }

    /// Read the phase of one restart without changing its state.
    pub async fn service_restart_status(&self, restart_id: i64) -> Result<Value> {
        self.read(move |store| {
            let phase: Option<String> = store.conn().query_row(
                "SELECT phase FROM service_restarts WHERE id=?", [restart_id], |row|row.get(0),
            ).optional()?;
            Ok(json!({"restart_id":restart_id,"phase":phase.ok_or_else(|| invalid("unknown service restart"))?}))
        }).await
    }

    /// Retain a blocked caller's session for the restart completion notice.
    pub async fn remember_service_restart_caller(
        &self,
        restart_id: i64,
        reference: OrchestratorRef,
    ) -> Result<()> {
        self.lane("service-restart")?
            .local(
                agent_run_store::ops::RememberServiceRestartCaller {
                    restart_id,
                    reference,
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await
    }

    /// Preserve a known caller when a start was refused by the restart barrier.
    async fn remember_blocked_start(
        &self,
        error: &Error,
        caller: Option<OrchestratorRef>,
    ) -> Result<()> {
        if let (Error::Transition(message), Some(reference)) = (error, caller)
            && let Some(restart_id) = agent_run_store::service_restart::blocked_restart_id(message)
        {
            self.remember_service_restart_caller(restart_id, reference)
                .await?;
        }
        Ok(())
    }
    /// Borrow bounded advisory read counters. Poison discards and restores the
    /// entire hint cache; durable notices still deduplicate corrective input.
    /// This cache never authenticates a call or authorizes native work.
    pub(crate) fn pool_read_guards(
        &self,
    ) -> std::sync::MutexGuard<'_, crate::dispatch::PoolReadGuards> {
        self.pool_reads.lock().unwrap_or_else(|poison| {
            let mut state = poison.into_inner();
            *state = crate::dispatch::PoolReadGuards::default();
            self.pool_reads.clear_poison();
            state
        })
    }

    /// Nonblocking writer queue and exact current/last callback observations.
    /// Absent on client-only services; never opens a database or mutates state.
    pub fn writer_health(&self) -> Option<agent_run_domain::storage::WriterHealth> {
        self.writer.as_ref().map(|writer| writer.handle().health())
    }

    /// Creates a service whose configuration is loaded lazily on first use.
    /// Clones share one cache. File access and validation errors are returned
    /// by [`Self::refresh_config`] or the operation using configuration.
    pub fn new(home: PathBuf) -> Self {
        Self {
            home,
            config: Arc::new(RwLock::new(None)),
            writer: None,
            system_launcher: None,
            pool_reads: Arc::default(),
        }
    }

    /// Attaches the resident writer every mutation of this service goes through.
    #[must_use]
    pub fn with_writer(mut self, writer: BrokerWriter) -> Self {
        self.writer = Some(writer);
        self
    }

    /// Selects a configured external launcher for new provider executions.
    /// Every admitted provider must have a matching route.
    #[must_use]
    pub fn with_system_launcher(
        mut self,
        launcher: Arc<dyn crate::system_launch::SystemLauncher>,
    ) -> Self {
        self.system_launcher = Some(launcher);
        self
    }

    /// Freezes the configured system route into a provider admission, if enabled.
    fn bind_system_route(&self, prepared: &mut Prepared) -> Result<()> {
        if let Some(launcher) = &self.system_launcher {
            let route = launcher.route(&prepared.authority.provider, prepared.authority.harness)?;
            route.validate_for(&prepared.authority.provider, prepared.authority.harness)?;
            let identity = prepared.identity.as_object_mut().ok_or_else(|| {
                Error::Integrity("provider launch identity is not an object".into())
            })?;
            identity.insert("system_route".into(), serde_json::to_value(route)?);
        }
        Ok(())
    }

    /// Uses only the launcher matching the execution's durable system route.
    async fn launch_provider_supervisor(&self, id: &AgentId, lane: &BrokerLane) -> Result<()> {
        let lookup = id.clone();
        let row = self.read(move |store| store.get(&lookup)).await?;
        let identity = ProviderLaunchIdentity::read(&row)?;
        match (&self.system_launcher, identity.system_route) {
            (None, None) => supervisor::launch(&self.home, id, lane).await,
            (Some(launcher), Some(route)) => {
                route.validate_for(&identity.authority.provider, identity.authority.harness)?;
                let current =
                    launcher.route(&identity.authority.provider, identity.authority.harness)?;
                if current != route {
                    return Err(Error::ExecutionChanged);
                }
                let future = launcher.launch(self.home.clone(), id.clone(), route, lane.clone());
                tokio::spawn(future).await.map_err(|error| {
                    Error::Runtime(format!("system supervisor launch task failed: {error}"))
                })?
            }
            _ => Err(Error::Unsupported(
                "execution belongs to a different launcher mode".into(),
            )),
        }
    }

    /// The broker lane of one subsystem.
    ///
    /// # Errors
    /// [`Error::BrokerUnavailable`] when no writer is attached.
    pub fn lane(&self, scope: &str) -> Result<BrokerLane> {
        self.writer
            .as_ref()
            .map(|writer| writer.lane(scope))
            .ok_or(Error::BrokerUnavailable)
    }

    /// Reloads `config.toml` only when its content digest changed.
    ///
    /// Returns `true` after installing a new valid revision and `false` when
    /// the exact bytes are unchanged. A malformed change returns an error and
    /// leaves the last valid revision intact for diagnostics; operations that
    /// need current configuration still reject that malformed change. Adoption
    /// and rejection diagnostics are offered to the configured logger at
    /// `Info` and `Warning` respectively, naming the SHA-256 revision that
    /// remains active; the logger's effective threshold controls persistence.
    /// An adoption is emitted only after the snapshot is installed, using the
    /// revision read back from the cache, so it never names an inactive revision.
    pub fn refresh_config(&self) -> Result<bool> {
        let mut cache = self
            .config
            .write()
            .map_err(|_| Error::Runtime("configuration cache lock is poisoned".into()))?;
        let revision = cache.as_ref().map(|cached| cached.revision.as_str());
        let loaded = match ProviderConfig::load_if_changed(&self.home, revision) {
            Ok(None) => return Ok(false),
            Ok(Some((value, revision))) => Ok((CachedConfigValue::Providers(value), revision)),
            Err(provider_error) => match Config::load_if_changed(&self.home, revision) {
                Ok(None) => return Ok(false),
                Ok(Some((value, revision))) => Ok((CachedConfigValue::Legacy(value), revision)),
                Err(legacy_error) => Err(
                    if matches!(
                        cache.as_ref().map(|entry| &entry.value),
                        Some(CachedConfigValue::Providers(_))
                    ) {
                        provider_error
                    } else {
                        legacy_error
                    },
                ),
            },
        };
        match loaded {
            Ok((value, revision)) => {
                *cache = Some(CachedConfig { revision, value });
                logging::config_reload(true, cache.as_ref().map(|c| c.revision.as_str()));
                Ok(true)
            }
            Err(error) => {
                logging::config_reload(false, revision);
                Err(error)
            }
        }
    }

    /// Returns the current valid configuration after checking its file digest.
    fn current_config(&self) -> Result<Config> {
        self.refresh_config()?;
        self.config
            .read()
            .map_err(|_| Error::Runtime("configuration cache lock is poisoned".into()))?
            .as_ref()
            .and_then(|cached| match &cached.value {
                CachedConfigValue::Legacy(value) => Some(value.clone()),
                _ => None,
            })
            .ok_or_else(|| Error::Runtime("configuration cache is empty".into()))
    }

    /// Loads schema v2 through the same last-valid exact-byte cache used by
    /// historical starts; an invalid file never replaces the active value.
    pub(crate) fn current_provider_config(&self) -> Result<(ProviderConfig, String)> {
        self.refresh_config()?;
        self.config
            .read()
            .map_err(|_| Error::Runtime("configuration cache lock is poisoned".into()))?
            .as_ref()
            .and_then(|cached| match &cached.value {
                CachedConfigValue::Providers(value) => {
                    Some((value.clone(), cached.revision.clone()))
                }
                _ => None,
            })
            .ok_or_else(|| invalid("provider start requires schema_version 2"))
    }

    /// Evaluates current policy for a stored system execution without returning
    /// configuration, private paths, account identifiers or credential data.
    pub fn system_current_policy(&self, id: &AgentId) -> crate::system_launch::CurrentPolicy {
        use crate::system_launch::CurrentPolicy;
        let frozen = Store::open(&self.home)
            .and_then(|store| store.get(id))
            .and_then(|row| ProviderLaunchIdentity::read(&row));
        let Ok(frozen) = frozen else {
            return CurrentPolicy::Refused;
        };
        let (Some(launcher), Some(route)) = (&self.system_launcher, &frozen.system_route) else {
            return CurrentPolicy::Refused;
        };
        if route.state_home != self.home
            || launcher
                .route(&frozen.authority.provider, frozen.authority.harness)
                .ok()
                .as_ref()
                != Some(route)
        {
            return CurrentPolicy::Refused;
        }
        let Ok((current, _)) = self.current_provider_config() else {
            return CurrentPolicy::Unavailable;
        };
        if current_policy_permits(&current, &frozen, &frozen.provider_request).is_err() {
            return CurrentPolicy::Refused;
        }
        let bound = supervisor::current_bindings(&current, &frozen.authority);
        let accounts = &frozen.authority.eligible_accounts;
        let authority = match serde_json::to_value(&frozen.authority) {
            Ok(value) => value,
            Err(_) => return CurrentPolicy::Refused,
        };
        CurrentPolicy::Allowed {
            authority_sha256: agent_run_domain::canonical::sha256_hex(&authority, true),
            bound: accounts
                .iter()
                .map(|account| bound.contains(account))
                .collect(),
            unchanged: accounts
                .iter()
                .map(|account| supervisor::ordered_binding_unchanged(&current, account, &frozen))
                .collect(),
        }
    }

    /// Admits a strict provider request using one fresh Quota snapshot and local policy.
    ///
    /// The orchestrator chooses provider, model, effort and profile; this
    /// only picks the account. Order of work:
    ///
    /// 1. A repeated `request_id` with the identical request returns the
    ///    original admission (`created=false`) before any config, account or
    ///    quota read; a different request under that id is `Conflict`.
    /// 2. The current valid v2 config and account registry resolve the
    ///    launch authority once; that config revision is frozen into the row.
    /// 3. Candidates combine Quota with local account state outside the write
    ///    transaction; an account label pins without failover.
    /// 4. `selection_stale` obtains a new snapshot and resubmits:
    ///    one initial selection plus at most [`PROVIDER_STALE_RETRIES`]
    ///    recalculations. If the last of those four submissions is also
    ///    stale, this returns `selection_busy` and nothing was admitted.
    ///    There is no sleep or polling; every other error returns as is.
    ///
    /// Test-only (feature `test-fixtures`): when the broker process runs with
    /// `AGENT_RUN_FIXTURE_ALWAYS_STALE` set, the committed capacity revision
    /// advances between every ranking and its submission, so a real broker
    /// deterministically spends its stale-retry budget (`selection_busy`).
    /// Absent in production builds.
    pub async fn admit_provider(&self, request: ProviderStartRequest) -> Result<Value> {
        self.admit_provider_ranked(request, &mut |_| Ok(())).await
    }

    /// [`Self::admit_provider`] with a hook run after each candidate set is
    /// computed and before its admission transaction, receiving the
    /// zero-based submission number.
    ///
    /// Test-only seam (feature `test-fixtures`) for deterministic races (a
    /// revision advance, an account disable) between the ranking read and
    /// the store transaction. The hook cannot see or change candidates; its
    /// error aborts the call unchanged.
    #[cfg(feature = "test-fixtures")]
    pub async fn admit_provider_observed(
        &self,
        request: ProviderStartRequest,
        before_admission: &mut (dyn FnMut(u32) -> Result<()> + Send),
    ) -> Result<Value> {
        self.admit_provider_ranked(request, before_admission).await
    }

    /// Ranked admission shared by [`Self::admit_provider`] and its test seam;
    /// `before_admission` runs between each ranking and its submission.
    async fn admit_provider_ranked(
        &self,
        request: ProviderStartRequest,
        before_admission: &mut (dyn FnMut(u32) -> Result<()> + Send),
    ) -> Result<Value> {
        self.admit_provider_with(request, PROVIDER_STALE_RETRIES, None, &mut |attempt, _| {
            before_admission(attempt)
        })
        .await
    }

    /// Test seam for a writer-side clock change after candidates are built.
    #[cfg(feature = "test-fixtures")]
    pub async fn admit_provider_decision_observed(
        &self,
        request: ProviderStartRequest,
        before_admission: &mut (dyn FnMut(u32, &mut QuotaCandidateSet) -> Result<()> + Send),
    ) -> Result<Value> {
        self.admit_provider_with(request, PROVIDER_STALE_RETRIES, None, before_admission)
            .await
    }

    /// Admits a strict provider request from trusted Rust quota candidates.
    /// The split lets offline tests drive the real supervisor executable
    /// without asking a test binary to respawn itself as `agent-run`.
    /// No transport accepts candidates from caller JSON. A fixed set cannot
    /// become fresh, so `selection_stale` returns without retry.
    pub async fn admit_provider_trusted(
        &self,
        request: ProviderStartRequest,
        candidates: QuotaCandidateSet,
    ) -> Result<Value> {
        self.admit_provider_with(request, 0, Some(candidates), &mut |_, _| Ok(()))
            .await
    }

    /// Shared admission: replay first, authority once, then up to
    /// `stale_retries + 1` submissions of freshly produced candidates.
    /// Validates/canonicalizes live paths when possible; a failed path check is
    /// enforced only for a new admission so exact retained intent can replay.
    ///
    /// Trusted fixtures may supply a fixed set; ordinary calls obtain fresh Quota
    /// before a bounded local read. The hook runs before each writer submission.
    async fn admit_provider_with(
        &self,
        mut request: ProviderStartRequest,
        stale_retries: u32,
        trusted: Option<QuotaCandidateSet>,
        before_admission: &mut (dyn FnMut(u32, &mut QuotaCandidateSet) -> Result<()> + Send),
    ) -> Result<Value> {
        request.validate_intent()?;
        // Preserve canonical spelling for live paths, but an exact immutable
        // retry remains valid after its workdir/read roots have disappeared.
        let paths = request.validate();
        if let Some(replay) = Store::open(&self.home)?.replay_provider_request(&request)? {
            let store = Store::open(&self.home)?;
            let row = store.get(&replay.agent_id)?;
            return Ok(
                json!({"agent_id":replay.agent_id,"attempt_id":replay.attempt_id,
                "created":false,"agent":self.view(&store,&row)?}),
            );
        }
        if let Some(restart_id) = agent_run_store::service_restart::active_service_restart(
            &Store::open(&self.home)?.conn,
        )? {
            return Err(Error::Transition(
                agent_run_store::service_restart::blocked_message(restart_id),
            ));
        }
        paths?;
        let (config, revision) = self.current_provider_config()?;
        let accounts = Store::open(&self.home)?.list_accounts()?;
        let catalog = config.resolve_catalog(accounts)?;
        let mut prepared = prepare_provider(&config, &revision, &catalog, request)?;
        self.bind_system_route(&mut prepared)?;
        let Prepared {
            request,
            effective,
            authority,
            identity,
            cap,
            pinned,
        } = prepared;
        let global_cap = config.core.max_active_agents;
        let live_quota = trusted.is_none();
        let mut submission = 0;
        let mut decisions = crate::capacity::quota_adapter::DecisionSnapshots::default();
        let admission = loop {
            let snapshot = if live_quota {
                decisions.read(config.capacity.quota.as_ref()).await
            } else {
                None
            };
            let mut candidates = if let Some(candidates) = &trusted {
                candidates.clone()
            } else {
                let catalog = catalog.clone();
                let request = request.clone();
                self.read(move |store| {
                    let pin = request.account.as_ref().map(|label| label.as_str());
                    Ok(crate::capacity::quota_adapter::candidates_at(
                        store,
                        &catalog,
                        &request.provider,
                        &request.model,
                        pin,
                        &std::collections::BTreeSet::new(),
                        snapshot.as_deref(),
                        now(),
                    )?
                    .candidates)
                })
                .await?
            };
            before_admission(submission, &mut candidates)?;
            #[cfg(feature = "test-fixtures")]
            if std::env::var_os("AGENT_RUN_FIXTURE_ALWAYS_STALE").is_some() {
                self.lane("admission")?
                    .handle()
                    .fixture_advance_quota_revision()
                    .await?;
            }
            match self
                .lane("admission")?
                .local(
                    agent_run_store::ops::AdmitProvider {
                        catalog: catalog.clone(),
                        inputs: agent_run_store::ops::AdmissionSpec {
                            request: request.clone(),
                            effective: effective.clone(),
                            authority: authority.clone(),
                            candidates,
                            identity: identity.clone(),
                            global_cap,
                            harness_cap: cap,
                            pinned: pinned.clone(),
                        },
                    },
                    crate::broker_writer::BROKER_BUDGET,
                )
                .await
            {
                Err(Error::QuotaAdmission(
                    QuotaAdmissionError::QuotaClockRollback
                    | QuotaAdmissionError::QuotaDecisionInvalid,
                )) if live_quota && decisions.retry_invalidated(&mut submission, stale_retries) => {
                }
                Err(Error::QuotaAdmission(QuotaAdmissionError::QuotaDecisionStale))
                    if live_quota && decisions.retry_expired(&mut submission, stale_retries) => {}
                Err(Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. }))
                    if stale_retries > 0 && submission == stale_retries =>
                {
                    return Err(QuotaAdmissionError::SelectionBusy { stale_retries }.into());
                }
                Err(Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. }))
                    if submission < stale_retries =>
                {
                    submission += 1;
                    decisions.require_fresh();
                }
                result => break result?,
            }
        };
        let store = Store::open(&self.home)?;
        let row = store.get(&admission.agent_id)?;
        Ok(
            json!({"agent_id":admission.agent_id,"attempt_id":admission.attempt_id,
            "created":admission.created,"agent":self.view(&store,&row)?}),
        )
    }

    /// Admits through [`Self::admit_provider`] and hands a newly owned attempt
    /// to the normal detached provider-aware supervisor; a replayed admission
    /// (`created=false`) never launches another child or reserves again.
    pub async fn start_provider(&self, request: ProviderStartRequest) -> Result<Value> {
        let caller = request.orchestrator.clone();
        let mut result = match self.admit_provider(request).await {
            Ok(value) => value,
            Err(error) => {
                self.remember_blocked_start(&error, caller).await?;
                return Err(error);
            }
        };
        self.hand_off_provider(&mut result).await?;
        Ok(result)
    }

    /// Atomically admits independent provider jobs, retaining their replay receipt.
    /// An exact replay uses no mutable config, paths, quota or supervisor launch.
    pub async fn admit_batch(
        &self,
        request: agent_run_domain::batch::StartBatchRequest,
    ) -> Result<agent_run_domain::batch::StartBatchResult> {
        self.admit_batch_ranked(request, &mut |_, _| Ok(())).await
    }

    /// Exercises a local revision race after each complete batch candidate read.
    #[cfg(feature = "test-fixtures")]
    pub async fn admit_batch_observed(
        &self,
        request: agent_run_domain::batch::StartBatchRequest,
        before_admission: &mut (dyn FnMut(u32) -> Result<()> + Send),
    ) -> Result<agent_run_domain::batch::StartBatchResult> {
        self.admit_batch_ranked(request, &mut |attempt, _| before_admission(attempt))
            .await
    }

    /// Test seam for a writer-side clock change after all batch candidates are built.
    #[cfg(feature = "test-fixtures")]
    pub async fn admit_batch_decision_observed(
        &self,
        request: agent_run_domain::batch::StartBatchRequest,
        before_admission: &mut (
                 dyn FnMut(u32, &mut [agent_run_store::ops::BatchMemberSpec]) -> Result<()> + Send
             ),
    ) -> Result<agent_run_domain::batch::StartBatchResult> {
        self.admit_batch_ranked(request, before_admission).await
    }

    /// Shares replay-first batch admission and its optional deterministic race hook.
    async fn admit_batch_ranked(
        &self,
        mut request: agent_run_domain::batch::StartBatchRequest,
        before_admission: &mut (
                 dyn FnMut(u32, &mut [agent_run_store::ops::BatchMemberSpec]) -> Result<()> + Send
             ),
    ) -> Result<agent_run_domain::batch::StartBatchResult> {
        request.validate_public_key()?;
        request.validate_intent()?;
        let fingerprint = request.fingerprint()?;
        if let Some(replay) = Store::open(&self.home)?.replay_batch_scoped(
            &request.request_id,
            &fingerprint,
            request.expected_store_id.as_ref(),
        )? {
            replay
                .validate_for_request(&request)
                .map_err(|_| Error::Integrity("invalid batch replay receipt".into()))?;
            return Ok(replay);
        }
        request.validate_new_lookup_frame()?;
        if let Some(restart_id) = agent_run_store::service_restart::active_service_restart(
            &Store::open(&self.home)?.conn,
        )? {
            return Err(Error::Transition(
                agent_run_store::service_restart::blocked_message(restart_id),
            ));
        }
        let (config, revision) = self.current_provider_config()?;
        let accounts = Store::open(&self.home)?.list_accounts()?;
        let catalog = config.resolve_catalog(accounts)?;
        let mut prepared = Vec::with_capacity(request.jobs.len());
        for mut job in request.jobs {
            job.validate()?;
            let mut job = prepare_provider(&config, &revision, &catalog, job)?;
            self.bind_system_route(&mut job)?;
            prepared.push(job);
        }
        let ids: Vec<_> = prepared.iter().map(|_| AgentId::new()).collect();
        let mut submission = 0;
        let mut decisions = crate::capacity::quota_adapter::DecisionSnapshots::default();
        loop {
            // One external snapshot serves every job in this attempt; never hold
            // a local transaction while awaiting another service.
            let snapshot = decisions.read(config.capacity.quota.as_ref()).await;
            let requests: Vec<_> = prepared.iter().map(|job| job.request.clone()).collect();
            let rank_catalog = catalog.clone();
            let sets = self
                .read(move |store| {
                    let read = store.conn.unchecked_transaction()?;
                    let decision_time = now();
                    let mut sets = Vec::with_capacity(requests.len());
                    for request in &requests {
                        let pin = request.account.as_ref().map(|label| label.as_str());
                        sets.push(
                            crate::capacity::quota_adapter::candidates_at(
                                store,
                                &rank_catalog,
                                &request.provider,
                                &request.model,
                                pin,
                                &std::collections::BTreeSet::new(),
                                snapshot.as_deref(),
                                decision_time,
                            )?
                            .candidates,
                        );
                    }
                    drop(read);
                    Ok(sets)
                })
                .await?;
            let mut jobs = Vec::with_capacity(prepared.len());
            for ((id, job), candidates) in ids.iter().zip(&prepared).zip(&sets) {
                jobs.push(agent_run_store::ops::BatchMemberSpec {
                    id: id.clone(),
                    inputs: AdmissionInputs {
                        request: &job.request,
                        effective: &job.effective,
                        authority: &job.authority,
                        candidates,
                        identity: &job.identity,
                        global_cap: config.core.max_active_agents,
                        harness_cap: job.cap,
                        pinned: job.pinned.as_ref(),
                    }
                    .into(),
                });
            }
            before_admission(submission, &mut jobs)?;
            let outcome = self
                .lane("admission")?
                .local(
                    agent_run_store::ops::AdmitBatch {
                        request_id: request.request_id.clone(),
                        fingerprint: fingerprint.clone(),
                        retain_history: request.retain_history,
                        expected_store_id: request.expected_store_id.clone(),
                        preconditions: request.preconditions.clone(),
                        catalog: catalog.clone(),
                        jobs,
                    },
                    crate::broker_writer::BROKER_BUDGET,
                )
                .await;
            match outcome {
                Err(Error::QuotaAdmission(
                    QuotaAdmissionError::QuotaClockRollback
                    | QuotaAdmissionError::QuotaDecisionInvalid,
                )) if decisions.retry_invalidated(&mut submission, PROVIDER_STALE_RETRIES) => {}
                Err(Error::QuotaAdmission(QuotaAdmissionError::QuotaDecisionStale))
                    if decisions.retry_expired(&mut submission, PROVIDER_STALE_RETRIES) => {}
                Err(Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. }))
                    if submission < PROVIDER_STALE_RETRIES =>
                {
                    submission += 1;
                    decisions.require_fresh();
                }
                Err(Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. })) => {
                    return Err(QuotaAdmissionError::SelectionBusy {
                        stale_retries: PROVIDER_STALE_RETRIES,
                    }
                    .into());
                }
                Err(error) if batch_admission_unknown(&error) => {
                    return Err(Error::BatchOutcomeUnknown {
                        source: Box::new(error),
                    });
                }
                result => return result,
            }
        }
    }

    /// Admit one private Pool enrollment through the same provider preparation,
    /// quota snapshot and batch writer as ordinary starts.
    pub(crate) async fn start_managed_enrollment(
        &self,
        grant: &crate::attempt_binding::PeerGrant,
        request: agent_run_domain::enrollment::Start,
        raw: Value,
        fingerprint: String,
    ) -> Result<agent_run_domain::enrollment::Receipt> {
        use agent_run_domain::{batch::StartBatchRequest, enrollment::Key};
        use agent_run_store::ops::{AdmitManagedEnrollment, PrepareManagedEnrollment};
        let key = Key {
            version: 1,
            expected_store_id: request.expected_store_id.clone(),
            pool_store_id: request.pool_store_id.clone(),
            request_id: request.request_id.clone(),
            fingerprint,
        };
        let route = self.managed_enrollment_grant(grant)?;
        key.validate(&route.pool_route)?;
        let lookup_key = key.clone();
        let lookup_route = route.pool_route.clone();
        if let Some(existing) = self
            .read(move |store| store.managed_enrollment_lookup(&lookup_key, &lookup_route))
            .await?
        {
            if existing
                .grant
                .as_ref()
                .is_some_and(|original| original != &route)
            {
                return Err(invalid("enrollment transport changed"));
            }
            if let Some(receipt) = existing.receipt {
                return Ok(receipt);
            }
            if existing.state == "aborted" {
                return Err(invalid("enrollment aborted"));
            }
        }
        self.attempt_enabled(grant)?;
        let prepared_key = key.clone();
        let prepared_route = route.clone();
        let stage = self
            .lane("admission")?
            .local(
                PrepareManagedEnrollment {
                    key: prepared_key,
                    grant: prepared_route,
                    request: raw,
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await?;
        if let Some(receipt) = stage.receipt {
            return Ok(receipt);
        }
        if stage.state == "aborted" {
            return Err(invalid("enrollment aborted"));
        }
        let (config, revision) = self.current_provider_config()?;
        let settings = config
            .managed_attempt_binding
            .as_ref()
            .filter(|settings| settings.enabled)
            .ok_or_else(|| Error::Unsupported("managed attempt binding disabled".into()))?;
        if request
            .jobs
            .iter()
            .any(|job| job.selector.role_name != settings.role_name)
        {
            return Err(invalid("enrollment role mismatch"));
        }
        let accounts = Store::open(&self.home)?.list_accounts()?;
        let catalog = config.resolve_catalog(accounts)?;
        let mut jobs = Vec::with_capacity(request.jobs.len());
        let mut role_digests = Vec::with_capacity(request.jobs.len());
        let mut normalized = Vec::with_capacity(request.jobs.len());
        for member in &request.jobs {
            let mut spec: ProviderStartRequest = serde_json::from_value(member.spec.clone())
                .map_err(|_| invalid("enrollment request invalid"))?;
            spec.validate_intent()?;
            normalized.push(spec.clone());
            spec.validate()?;
            let mut prepared = prepare_provider(&config, &revision, &catalog, spec)?;
            let role = role_plan::ResolvedRolePlan::from_payload(&prepared.authority.role_payload)
                .map_err(|_| invalid("enrollment role unqualified"))?;
            let selected = adapters::managed_role::select(
                &config,
                prepared.authority.harness,
                &role,
                prepared.request.explicit_finish,
            )
            .map_err(|_| invalid("enrollment role unqualified"))?
            .ok_or_else(|| invalid("enrollment role unqualified"))?;
            if role.role_name != member.selector.role_name
                || prepared.request.write
                || prepared.effective.write
            {
                return Err(invalid("enrollment role mismatch"));
            }
            role_digests.push(selected.role_digest);
            self.bind_system_route(&mut prepared)?;
            jobs.push(prepared);
        }
        // Namespace the existing batch replay key by the authenticated Pool route.
        let internal = format!(
            "p3_{}",
            agent_run_domain::canonical::sha256_hex(
                &json!([route.pool_route, key.pool_store_id, key.request_id]),
                true
            )
        );
        let mut batch = StartBatchRequest {
            request_id: internal,
            jobs: normalized,
            retain_history: true,
            expected_store_id: Some(request.expected_store_id.clone()),
            preconditions: (!request.preconditions.is_empty())
                .then_some(request.preconditions.clone()),
        };
        batch.validate_intent()?;
        batch.validate_new_lookup_frame()?;
        let batch_fingerprint = batch.fingerprint()?;
        let ids: Vec<_> = jobs.iter().map(|_| AgentId::new()).collect();
        let mut submission = 0;
        let mut decisions = crate::capacity::quota_adapter::DecisionSnapshots::default();
        let outcome = loop {
            let snapshot = decisions.read(config.capacity.quota.as_ref()).await;
            let requests: Vec<_> = jobs.iter().map(|job| job.request.clone()).collect();
            let rank_catalog = catalog.clone();
            let sets = self
                .read(move |store| {
                    let read = store.conn.unchecked_transaction()?;
                    let decision_time = now();
                    let mut sets = Vec::with_capacity(requests.len());
                    for request in &requests {
                        let pin = request.account.as_ref().map(|label| label.as_str());
                        sets.push(
                            crate::capacity::quota_adapter::candidates_at(
                                store,
                                &rank_catalog,
                                &request.provider,
                                &request.model,
                                pin,
                                &std::collections::BTreeSet::new(),
                                snapshot.as_deref(),
                                decision_time,
                            )?
                            .candidates,
                        );
                    }
                    drop(read);
                    Ok(sets)
                })
                .await?;
            let mut members = Vec::with_capacity(jobs.len());
            for ((id, job), candidates) in ids.iter().zip(&jobs).zip(&sets) {
                members.push(agent_run_store::ops::BatchMemberSpec {
                    id: id.clone(),
                    inputs: AdmissionInputs {
                        request: &job.request,
                        effective: &job.effective,
                        authority: &job.authority,
                        candidates,
                        identity: &job.identity,
                        global_cap: config.core.max_active_agents,
                        harness_cap: job.cap,
                        pinned: job.pinned.as_ref(),
                    }
                    .into(),
                });
            }
            let attempt = self
                .lane("admission")?
                .local(
                    AdmitManagedEnrollment {
                        key: key.clone(),
                        grant: route.clone(),
                        config_revision: revision.clone(),
                        role_digests: role_digests.clone(),
                        batch: agent_run_store::ops::AdmitBatch {
                            request_id: batch.request_id.clone(),
                            fingerprint: batch_fingerprint.clone(),
                            retain_history: true,
                            expected_store_id: batch.expected_store_id.clone(),
                            preconditions: batch.preconditions.clone(),
                            catalog: catalog.clone(),
                            jobs: members,
                        },
                    },
                    crate::broker_writer::BROKER_BUDGET,
                )
                .await;
            match attempt {
                Err(Error::QuotaAdmission(
                    QuotaAdmissionError::QuotaClockRollback
                    | QuotaAdmissionError::QuotaDecisionInvalid,
                )) if decisions.retry_invalidated(&mut submission, PROVIDER_STALE_RETRIES) => {}
                Err(Error::QuotaAdmission(QuotaAdmissionError::QuotaDecisionStale))
                    if decisions.retry_expired(&mut submission, PROVIDER_STALE_RETRIES) => {}
                Err(Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. }))
                    if submission < PROVIDER_STALE_RETRIES =>
                {
                    submission += 1;
                    decisions.require_fresh();
                }
                Err(Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. })) => {
                    return Err(QuotaAdmissionError::SelectionBusy {
                        stale_retries: PROVIDER_STALE_RETRIES,
                    }
                    .into());
                }
                Err(error) if batch_admission_unknown(&error) => {
                    return Err(Error::BatchOutcomeUnknown {
                        source: Box::new(error),
                    });
                }
                other => break other?,
            }
        };
        if !outcome.replayed {
            for job in &outcome.receipt.jobs {
                self.hand_off_admitted(&job.execution_id).await;
            }
        }
        Ok(outcome.receipt)
    }

    /// Admits a batch, then hands off only newly admitted jobs to supervisors.
    /// The transport owns this future across client timeout; the receipt does
    /// not promise that any process has started.
    pub async fn start_batch(
        &self,
        request: agent_run_domain::batch::StartBatchRequest,
    ) -> Result<agent_run_domain::batch::StartBatchResult> {
        let result = self.start_batch_owned(request).await;
        if let Err(error) = &result
            && let Some(logger) = logging::configured()
        {
            logger.log(
                logging::Level::Warning,
                &format!("batch_start_incomplete class={}", error.public().kind),
            );
        }
        result
    }

    /// Owns admission and every post-commit handoff independently of a client wait.
    async fn start_batch_owned(
        &self,
        request: agent_run_domain::batch::StartBatchRequest,
    ) -> Result<agent_run_domain::batch::StartBatchResult> {
        let callers: Vec<_> = request
            .jobs
            .iter()
            .filter_map(|job| job.orchestrator.clone())
            .collect();
        let result = match self.admit_batch(request).await {
            Ok(result) => result,
            Err(error) => {
                self.remember_blocked_batch(&error, callers).await?;
                return Err(error);
            }
        };
        if !result.replayed {
            for job in &result.receipt.jobs {
                self.hand_off_admitted(&job.agent_id).await;
            }
        }
        Ok(result)
    }

    /// Hand one newly admitted job to its supervisor. The complete handoff,
    /// including synchronous launcher code, is isolated in its own task: a
    /// panic or cancellation says nothing about whether a child was created,
    /// so it is durably recorded as `supervisor_handoff_error` and the caller
    /// continues with the next job.
    async fn hand_off_admitted(&self, id: &AgentId) {
        let service = self.clone();
        let owned = id.clone();
        let handoff = tokio::spawn(async move {
            let mut single = json!({"created":true,"agent_id":owned});
            service.hand_off_provider(&mut single).await
        });
        match handoff.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => batch_handoff_log(id, error.public().kind),
            Err(error) => {
                let reason = if error.is_panic() {
                    "panic"
                } else {
                    "cancelled"
                };
                let recorded = match self.lane("admission") {
                    Ok(lane) => {
                        lane.event(
                            id,
                            "supervisor_handoff_error",
                            json!({"kind":"RuntimeError","reason":reason}),
                        )
                        .await
                    }
                    Err(error) => Err(error),
                };
                if let Err(error) = recorded {
                    batch_handoff_log(id, error.public().kind);
                }
                batch_handoff_log(id, "unknown");
            }
        }
    }

    /// Record each distinct explicitly bound caller refused by a restart fence.
    async fn remember_blocked_batch(
        &self,
        error: &Error,
        callers: Vec<OrchestratorRef>,
    ) -> Result<()> {
        if !matches!(error, Error::Transition(_)) {
            return Ok(());
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut failure = None;
        for caller in callers {
            let key = (
                caller.canonical_transport()?,
                caller.external_session_id.clone(),
            );
            if seen.insert(key)
                && let Err(error) = self.remember_blocked_start(error, Some(caller)).await
                && failure.is_none()
            {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    /// Launches the supervisor for a newly created provider admission only;
    /// a launch failure is recorded as a `supervisor_handoff_error` event.
    ///
    /// A definite pre-spawn failure (`Error::Io`: the OS refused the spawn,
    /// or the child died before its PID proof and so owns nothing) certifies
    /// the still-prepared attempt as never spawned, ends the run `failed`
    /// with `supervisor_spawn_failed`, completes its terminal delivery once,
    /// and refreshes `result["agent"]` so the caller returns the durable
    /// terminal view. Any other launch failure is ambiguous: ownership is
    /// retained for reconciliation. A replay (`created=false`) launches
    /// nothing.
    async fn hand_off_provider(&self, result: &mut Value) -> Result<()> {
        if result["created"] == true {
            let id: AgentId = serde_json::from_value(result["agent_id"].clone())?;
            let lane = self.lane("admission")?;
            if let Err(error) = self.launch_provider_supervisor(&id, &lane).await {
                lane.event(
                    &id,
                    "supervisor_handoff_error",
                    json!({"kind":error.public().kind}),
                )
                .await?;
                let never_spawned = matches!(error, Error::Io(_)) && {
                    let probe = id.clone();
                    self.read(move |store| {
                        crate::persist::attempt_never_spawned(&store.conn, &probe)
                    })
                    .await?
                };
                if never_spawned {
                    let mut outcome = Outcome::failure("supervisor_spawn_failed");
                    outcome.failure_text = Some("the run supervisor could not be started".into());
                    lane.finish_terminal(&id, &outcome).await?;
                    result["agent"] = self.view_of(&id).await?;
                }
            }
        }
        Ok(())
    }

    /// Admits trusted quota candidates and hands a newly owned attempt to
    /// the normal detached supervisor; replay never launches another child.
    pub async fn start_provider_trusted(
        &self,
        request: ProviderStartRequest,
        candidates: QuotaCandidateSet,
    ) -> Result<Value> {
        let mut result = self.admit_provider_trusted(request, candidates).await?;
        self.hand_off_provider(&mut result).await?;
        Ok(result)
    }
    /// Admits a whole cooperative pool using one Quota snapshot per attempt.
    pub async fn admit_pool(&self, request: PoolStartRequest) -> Result<Value> {
        self.admit_pool_with(request, PROVIDER_STALE_RETRIES, None)
            .await
    }

    /// [`Self::admit_pool`] with one fixed trusted candidate set for every
    /// member, so offline tests can drive the real supervisor. A fixed set
    /// cannot become fresh: `selection_stale` returns without retry.
    pub async fn admit_pool_trusted(
        &self,
        request: PoolStartRequest,
        candidates: QuotaCandidateSet,
    ) -> Result<Value> {
        self.admit_pool_with(request, 0, Some(candidates)).await
    }

    /// Shared pool admission.
    ///
    /// 1. The outer client request is validated and normalized; its digest
    ///    (never the composed member tasks, which carry fresh identities)
    ///    scopes replay, so a repeat returns the original pool and stable
    ///    identities before any configuration or quota read, and a different
    ///    request under the same key is `Conflict`.
    /// 2. One configuration and catalog snapshot serves every member. The pool
    ///    and every agent identity are minted, each member's actual task is
    ///    composed from the common goal, its own seat and all peers, and each
    ///    composed request is prepared through the same mechanism as a single
    ///    start, so the stored task, effective request and frozen identity
    ///    agree exactly.
    /// 3. Candidates are produced outside any write transaction for every
    ///    member, then [`Store::admit_pool`] commits all rows or none.
    ///    `selection_stale` retries like a single start, over the whole batch.
    ///
    /// The result lists only stable identities, names, roles and statuses.
    async fn admit_pool_with(
        &self,
        mut request: PoolStartRequest,
        stale_retries: u32,
        trusted: Option<QuotaCandidateSet>,
    ) -> Result<Value> {
        request.validate()?;
        let request_value = serde_json::to_value(&request)?;
        let canonical_sha = agent_run_domain::canonical::sha256_hex(&request_value, true);
        let (namespace, sha) = match &request.orchestrator {
            None => ("global".to_owned(), canonical_sha),
            Some(reference) => {
                let canonical = reference.canonical_transport()?;
                let namespace = agent_run_domain::canonical::sha256_hex(
                    &json!([canonical, reference.external_session_id]),
                    true,
                );
                (namespace, canonical_sha)
            }
        };
        let mut replay_keys = vec![(namespace.clone(), sha.clone())];
        if let Some(reference) = &request.orchestrator {
            let alias = reference.legacy_transport()?;
            let mut compatible = request_value.clone();
            compatible["orchestrator"]["transport"] = json!(alias);
            replay_keys.push((
                agent_run_domain::canonical::sha256_hex(
                    &json!([alias, reference.external_session_id]),
                    true,
                ),
                agent_run_domain::canonical::sha256_hex(&compatible, true),
            ));
        }
        let store = Store::open(&self.home)?;
        for (namespace, sha) in replay_keys {
            if let Some(found) = store.replay_pool(&namespace, &request.request_id, &sha)? {
                return self.pool_view(&Store::open(&self.home)?, &found);
            }
        }
        if let Some(restart_id) =
            agent_run_store::service_restart::active_service_restart(&store.conn)?
        {
            return Err(Error::Transition(
                agent_run_store::service_restart::blocked_message(restart_id),
            ));
        }
        let (config, revision) = self.current_provider_config()?;
        let accounts = Store::open(&self.home)?.list_accounts()?;
        let catalog = config.resolve_catalog(accounts)?;
        let pool_id = PoolId::new();
        let existing = request
            .members
            .iter()
            .map(|member| {
                member
                    .existing_agent_id()
                    .map(|id| store.existing_pool_member(id))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        drop(store);
        let mut binding = request.orchestrator.clone();
        for pin in existing.iter().flatten() {
            if let Some(reference) = &pin.orchestrator {
                if let Some(current) = &binding {
                    if !agent_run_store::pool_enrollment::same_binding(
                        Some(current),
                        Some(reference),
                    )? {
                        return Err(invalid(
                            "existing workers must share the pool orchestrator binding",
                        ));
                    }
                } else {
                    binding = Some(reference.clone());
                }
            }
        }
        let seats: Vec<PoolSeat> = request
            .members
            .iter()
            .zip(&existing)
            .enumerate()
            .map(|(index, (member, pin))| {
                let slot = index as u8 + 1;
                PoolSeat {
                    slot,
                    name: member
                        .start()
                        .and_then(|start| start.display_name.clone())
                        .or_else(|| pin.as_ref().and_then(|p| p.name.clone()))
                        .unwrap_or_else(|| format!("{} {slot}", member.role())),
                    role: member.role().to_owned(),
                    agent_id: pin
                        .as_ref()
                        .map(|p| p.agent_id.clone())
                        .unwrap_or_else(AgentId::new),
                }
            })
            .collect();
        let pending = existing
            .iter()
            .flatten()
            .map(|pin| pin.agent_id.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let mut prepared = Vec::new();
        for (member, seat) in request.members.iter().zip(&seats) {
            if let Some(original) = member.start() {
                let mut start = original.clone();
                start.task = compose_member_task(
                    &pool_id,
                    &request.goal,
                    &request.acceptance,
                    &seats,
                    seat,
                    &original.task,
                    original.explicit_finish,
                )?;
                if !pending.is_empty() {
                    start.task.push_str(&format!("\nExisting seats pending authenticated enrollment acknowledgement: {pending}. They retain their current work and deadline; do not assume they read the context until joined.\n"));
                }
                start.orchestrator = binding.clone();
                start.validate()?;
                let mut member = prepare_provider(&config, &revision, &catalog, start)?;
                self.bind_system_route(&mut member)?;
                prepared.push(Some(member));
            } else {
                prepared.push(None);
            }
        }
        let live_quota = trusted.is_none();
        let mut submission = 0;
        let mut decisions = crate::capacity::quota_adapter::DecisionSnapshots::default();
        let admission = loop {
            let snapshot = if live_quota && prepared.iter().any(Option::is_some) {
                decisions.read(config.capacity.quota.as_ref()).await
            } else {
                None
            };
            let sets = if let Some(candidates) = &trusted {
                prepared
                    .iter()
                    .map(|member| member.as_ref().map(|_| candidates.clone()))
                    .collect()
            } else {
                let requests: Vec<_> = prepared
                    .iter()
                    .map(|member| member.as_ref().map(|p| p.request.clone()))
                    .collect();
                let rank_catalog = catalog.clone();
                self.read(move |store| {
                    let read = store.conn.unchecked_transaction()?;
                    let decision_time = now();
                    let mut sets = Vec::with_capacity(requests.len());
                    for request in &requests {
                        sets.push(
                            request
                                .as_ref()
                                .map(|request| {
                                    let pin = request.account.as_ref().map(|label| label.as_str());
                                    Ok::<_, Error>(
                                        crate::capacity::quota_adapter::candidates_at(
                                            store,
                                            &rank_catalog,
                                            &request.provider,
                                            &request.model,
                                            pin,
                                            &std::collections::BTreeSet::new(),
                                            snapshot.as_deref(),
                                            decision_time,
                                        )?
                                        .candidates,
                                    )
                                })
                                .transpose()?,
                        );
                    }
                    drop(read);
                    Ok(sets)
                })
                .await?
            };
            let members: Vec<PoolMemberAdmission<'_>> = request
                .members
                .iter()
                .zip(&seats)
                .zip(&prepared)
                .zip(&sets)
                .zip(&existing)
                .map(|((((member, seat), prepared), candidates), pin)| {
                    let source = if let (Some(p), Some(c)) =
                        (prepared.as_ref(), candidates.as_ref())
                    {
                        agent_run_store::pool_admission::PoolAdmissionSource::New(AdmissionInputs {
                            request: &p.request,
                            effective: &p.effective,
                            authority: &p.authority,
                            candidates: c,
                            identity: &p.identity,
                            global_cap: config.core.max_active_agents,
                            harness_cap: p.cap,
                            pinned: p.pinned.as_ref(),
                        })
                    } else {
                        agent_run_store::pool_admission::PoolAdmissionSource::Existing(
                            pin.as_ref().expect("validated existing source").clone(),
                        )
                    };
                    PoolMemberAdmission {
                        id: seat.agent_id.clone(),
                        slot: seat.slot,
                        name: seat.name.clone(),
                        role: seat.role.clone(),
                        personal_task: member
                            .start()
                            .map(|start| start.task.clone())
                            .unwrap_or_else(|| pin.as_ref().expect("existing source").task.clone()),
                        source,
                    }
                })
                .collect();
            let members: Vec<agent_run_store::ops::PoolMemberSpec> = members
                .into_iter()
                .map(
                    |member: PoolMemberAdmission<'_>| agent_run_store::ops::PoolMemberSpec {
                        id: member.id,
                        slot: member.slot,
                        name: member.name,
                        role: member.role,
                        personal_task: member.personal_task,
                        source: match member.source {
                            agent_run_store::pool_admission::PoolAdmissionSource::New(inputs) => {
                                agent_run_store::ops::PoolMemberSource::New(Box::new(inputs.into()))
                            }
                            agent_run_store::pool_admission::PoolAdmissionSource::Existing(pin) => {
                                agent_run_store::ops::PoolMemberSource::Existing(Box::new(pin))
                            }
                        },
                    },
                )
                .collect();
            let outcome = self
                .lane("pools")?
                .local(
                    agent_run_store::ops::AdmitPool {
                        pool_id: pool_id.clone(),
                        request_namespace: namespace.clone(),
                        request_id: request.request_id.clone(),
                        request_sha256: sha.clone(),
                        goal: request.goal.clone(),
                        acceptance: request.acceptance.clone(),
                        catalog: catalog.clone(),
                        members,
                        orchestrator: binding.clone(),
                    },
                    crate::broker_writer::BROKER_BUDGET,
                )
                .await;
            match outcome {
                Err(Error::QuotaAdmission(
                    QuotaAdmissionError::QuotaClockRollback
                    | QuotaAdmissionError::QuotaDecisionInvalid,
                )) if live_quota && decisions.retry_invalidated(&mut submission, stale_retries) => {
                }
                Err(Error::QuotaAdmission(QuotaAdmissionError::QuotaDecisionStale))
                    if live_quota && decisions.retry_expired(&mut submission, stale_retries) => {}
                Err(Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. }))
                    if stale_retries > 0 && submission == stale_retries =>
                {
                    return Err(QuotaAdmissionError::SelectionBusy { stale_retries }.into());
                }
                Err(Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. }))
                    if submission < stale_retries =>
                {
                    submission += 1;
                    decisions.require_fresh();
                }
                result => break result?,
            }
        };
        self.pool_view(&Store::open(&self.home)?, &admission)
    }

    /// Admits a pool through [`Self::admit_pool`] and hands every newly
    /// admitted member to the normal detached supervisor in slot order.
    ///
    /// All stable identities are durable before the first launch. A member
    /// whose launch fails ends terminally with its exact identity visible and
    /// the remaining members are still launched; a replay launches nothing.
    pub async fn start_pool(&self, request: PoolStartRequest) -> Result<Value> {
        let caller = request.orchestrator.clone();
        let result = match self.admit_pool(request).await {
            Ok(value) => value,
            Err(error) => {
                self.remember_blocked_start(&error, caller).await?;
                return Err(error);
            }
        };
        self.hand_off_pool(result).await
    }

    /// [`Self::start_pool`] over a fixed trusted candidate set.
    pub async fn start_pool_trusted(
        &self,
        request: PoolStartRequest,
        candidates: QuotaCandidateSet,
    ) -> Result<Value> {
        let result = self.admit_pool_trusted(request, candidates).await?;
        self.hand_off_pool(result).await
    }

    /// Appends an operator message to a pool: the author is stamped by the
    /// broker as the operator, the entry fans out to every current member's
    /// tip, and the same key replays. Refusals are typed [`PoolDenial`]s.
    pub async fn pool_post(
        &self,
        request: PoolPost,
    ) -> Result<std::result::Result<Value, PoolDenial>> {
        request.validate()?;
        let outcome = self
            .lane("pools")?
            .local(
                agent_run_store::ops::PoolOperatorPost {
                    pool_id: request.pool_id,
                    request_id: request.request_id,
                    message: request.message,
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await?;
        Ok(match outcome {
            agent_run_store::ops::PoolOutcome::Done(receipt) => Ok(json!({
                "pool_id": receipt.pool_id, "seq": receipt.seq, "created": !receipt.duplicate,
            })),
            agent_run_store::ops::PoolOutcome::Denied(denial) => Err(denial),
        })
    }

    /// Discovers a bounded page of pools without admission, settlement or writes.
    /// Strict page validation and frozen vote validity come from the store.
    pub fn list_pools(&self, query: agent_run_domain::pool::ListPoolsQuery) -> Result<Value> {
        query.validate()?;
        Ok(serde_json::to_value(
            Store::open(&self.home)?.list_pools(&query)?,
        )?)
    }

    /// Reads a pool's derived status and one cursor page of its log for the
    /// operator, through the projection members read.
    pub fn pool_status(&self, query: PoolQuery) -> Result<std::result::Result<Value, PoolDenial>> {
        query.validate()?;
        Store::open(&self.home)?.pool_operator_read(
            &query.pool_id,
            query.after_seq.unwrap_or(0),
            query.before_seq,
            query.limit.unwrap_or(agent_run_domain::pool::MAX_PAGE),
        )
    }

    /// Admits a member replacement atomically; nothing is launched.
    pub async fn admit_pool_replacement(
        &self,
        request: PoolReplace,
    ) -> Result<std::result::Result<PoolReplacement, PoolDenial>> {
        self.admit_replacement_with(request, PROVIDER_STALE_RETRIES, None)
            .await
    }

    /// [`Self::admit_pool_replacement`] over one fixed trusted candidate set.
    pub async fn admit_pool_replacement_trusted(
        &self,
        request: PoolReplace,
        candidates: QuotaCandidateSet,
    ) -> Result<std::result::Result<PoolReplacement, PoolDenial>> {
        self.admit_replacement_with(request, 0, Some(candidates))
            .await
    }

    /// Shared replacement admission.
    ///
    /// 1. The normalized outer request's digest scopes replay: the same key
    ///    returns the original new identity before any configuration or quota
    ///    read, even after later replacements; a different request is
    ///    `Conflict`.
    /// 2. The pool and the seat are read; an explicit `start` is an ordinary
    ///    request under the current catalog and grants, an omitted one is the
    ///    seat's original user spec restored from its frozen request (personal
    ///    task back, pool-owned binding and key removed), so no resolved
    ///    credential or automatically chosen account is carried over.
    /// 3. The new identity is minted, the task composed from the common goal,
    ///    the new roster and a catch-up line, and prepared like any start.
    /// 4. [`Store::replace_pool_member`] rechecks everything atomically; a
    ///    roster that moved meanwhile recomposes, a stale selection retries
    ///    like a single start.
    async fn admit_replacement_with(
        &self,
        mut request: PoolReplace,
        stale_retries: u32,
        trusted: Option<QuotaCandidateSet>,
    ) -> Result<std::result::Result<PoolReplacement, PoolDenial>> {
        request.validate()?;
        let sha = agent_run_domain::canonical::sha256_hex(&serde_json::to_value(&request)?, true);
        if let Some(found) = Store::open(&self.home)?.replay_pool_replacement(
            &request.pool_id,
            &request.request_id,
            &sha,
        )? {
            return Ok(found);
        }
        let (config, revision) = self.current_provider_config()?;
        let accounts = Store::open(&self.home)?.list_accounts()?;
        let catalog = config.resolve_catalog(accounts)?;
        let new_id = AgentId::new();
        let live_quota = trusted.is_none();
        let (mut submission, mut recomposed) = (0, 0);
        let mut decisions = crate::capacity::quota_adapter::DecisionSnapshots::default();
        loop {
            let store = Store::open(&self.home)?;
            let context = match store.pool_replace_context(&request.pool_id, &request.agent_id)? {
                Ok(context) => context,
                Err(denied) => return Ok(Err(denied)),
            };
            let frozen = ProviderLaunchIdentity::read(&store.get(&context.old.agent_id)?)?;
            let mut start = request.start.clone().unwrap_or_else(|| {
                let mut original = frozen.provider_request.clone();
                original.task = context.personal_task.clone();
                original
            });
            start.request_id = None;
            start.orchestrator = store.pool_binding(&request.pool_id)?;
            let name = start
                .display_name
                .clone()
                .unwrap_or_else(|| context.old.name.clone());
            start.display_name = Some(name.clone());
            let personal = start.task.clone();
            let seat = PoolSeat {
                slot: context.old.slot,
                name: name.clone(),
                role: context.old.role.clone(),
                agent_id: new_id.clone(),
            };
            let roster: Vec<PoolSeat> = context
                .seats
                .iter()
                .map(|member| {
                    if member.slot == seat.slot {
                        seat.clone()
                    } else {
                        PoolSeat {
                            slot: member.slot,
                            name: member.name.clone(),
                            role: member.role.clone(),
                            agent_id: member.agent_id.clone(),
                        }
                    }
                })
                .collect();
            let proposal = context.current_proposal.map_or(String::new(), |seq| {
                format!(" The current proposal is #{seq}.")
            });
            start.task = compose_member_task(
                &context.pool_id,
                &context.goal,
                &context.acceptance,
                &roster,
                &seat,
                &format!(
                    "{personal}\n\nRoster change: you replace {} ({}) in slot {} at roster \
                     revision {}. The pool log already holds entries up to #{}.{proposal} Call \
                     pool_read with after_seq=0 first, before acting.",
                    context.old.name,
                    context.old.agent_id,
                    seat.slot,
                    context.roster_revision + 1,
                    context.last_seq
                ),
                start.explicit_finish,
            )?;
            start.validate()?;
            let mut prepared = prepare_provider(&config, &revision, &catalog, start)?;
            self.bind_system_route(&mut prepared)?;
            drop(store);
            let snapshot = if live_quota {
                decisions.read(config.capacity.quota.as_ref()).await
            } else {
                None
            };
            let candidates = if let Some(candidates) = &trusted {
                candidates.clone()
            } else {
                let catalog = catalog.clone();
                let ranked_request = prepared.request.clone();
                self.read(move |store| {
                    let pin = ranked_request.account.as_ref().map(|label| label.as_str());
                    Ok(crate::capacity::quota_adapter::candidates_at(
                        store,
                        &catalog,
                        &ranked_request.provider,
                        &ranked_request.model,
                        pin,
                        &std::collections::BTreeSet::new(),
                        snapshot.as_deref(),
                        now(),
                    )?
                    .candidates)
                })
                .await?
            };
            let replacement = agent_run_store::ops::ReplacePoolMember {
                pool_id: request.pool_id.clone(),
                old: request.agent_id.clone(),
                request_id: request.request_id.clone(),
                request_sha256: sha.clone(),
                expected_roster_revision: context.roster_revision,
                new_id: new_id.clone(),
                name: name.clone(),
                personal_task: personal.clone(),
                catalog: catalog.clone(),
                inputs: AdmissionInputs {
                    request: &prepared.request,
                    effective: &prepared.effective,
                    authority: &prepared.authority,
                    candidates: &candidates,
                    identity: &prepared.identity,
                    global_cap: config.core.max_active_agents,
                    harness_cap: prepared.cap,
                    pinned: prepared.pinned.as_ref(),
                }
                .into(),
            };
            let outcome = self
                .lane("pools")?
                .local(replacement, crate::broker_writer::BROKER_BUDGET)
                .await
                .map(|outcome| match outcome {
                    agent_run_store::ops::PoolOutcome::Done(value) => Ok(value),
                    agent_run_store::ops::PoolOutcome::Denied(denial) => Err(denial),
                });
            match outcome {
                Ok(Err(PoolDenial::StaleRoster { .. })) if recomposed < 3 => {
                    recomposed += 1;
                    decisions.require_fresh();
                }
                Err(Error::QuotaAdmission(
                    QuotaAdmissionError::QuotaClockRollback
                    | QuotaAdmissionError::QuotaDecisionInvalid,
                )) if live_quota && decisions.retry_invalidated(&mut submission, stale_retries) => {
                }
                Err(Error::QuotaAdmission(QuotaAdmissionError::QuotaDecisionStale))
                    if live_quota && decisions.retry_expired(&mut submission, stale_retries) => {}
                Err(Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. }))
                    if stale_retries > 0 && submission == stale_retries =>
                {
                    return Err(QuotaAdmissionError::SelectionBusy { stale_retries }.into());
                }
                Err(Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. }))
                    if submission < stale_retries =>
                {
                    submission += 1;
                    decisions.require_fresh();
                }
                result => return result,
            }
        }
    }

    /// Replaces a pool member and hands the new execution to the normal
    /// detached supervisor after commit. The old retirement is never rolled
    /// back: a launch failure ends the new member terminally with its exact
    /// identity visible, and it can itself be replaced.
    pub async fn replace_pool_member(
        &self,
        request: PoolReplace,
    ) -> Result<std::result::Result<Value, PoolDenial>> {
        let admitted = self.admit_pool_replacement(request).await?;
        self.hand_off_replacement(admitted).await
    }

    /// [`Self::replace_pool_member`] over a fixed trusted candidate set.
    pub async fn replace_pool_member_trusted(
        &self,
        request: PoolReplace,
        candidates: QuotaCandidateSet,
    ) -> Result<std::result::Result<Value, PoolDenial>> {
        let admitted = self
            .admit_pool_replacement_trusted(request, candidates)
            .await?;
        self.hand_off_replacement(admitted).await
    }

    /// Launches a newly created replacement and renders the public result.
    async fn hand_off_replacement(
        &self,
        admitted: std::result::Result<PoolReplacement, PoolDenial>,
    ) -> Result<std::result::Result<Value, PoolDenial>> {
        let replacement = match admitted {
            Ok(replacement) => replacement,
            Err(denied) => return Ok(Err(denied)),
        };
        if replacement.created {
            let mut single = json!({"created": true, "agent_id": replacement.new.agent_id});
            let _ = self.hand_off_provider(&mut single).await;
        }
        let store = Store::open(&self.home)?;
        Ok(Ok(json!({
            "pool_id": replacement.pool_id,
            "created": replacement.created,
            "roster_revision": replacement.roster_revision,
            "replaced": {"agent_id": replacement.old.agent_id, "name": replacement.old.name,
                         "role": replacement.old.role},
            "member": {"agent_id": replacement.new.agent_id, "name": replacement.new.name,
                       "role": replacement.new.role,
                       "status": store.get(&replacement.new.agent_id)?.status.as_str()},
        })))
    }

    /// Launches only newly admitted members; existing workers retain their
    /// exact execution/session, then every status is read from its current tip.
    async fn hand_off_pool(&self, mut result: Value) -> Result<Value> {
        if result["created"] == true {
            let members = result["members"].as_array().cloned().unwrap_or_default();
            for member in &members {
                if member["existing"] == true {
                    continue;
                }
                let mut single = json!({"created": true, "agent_id": member["agent_id"]});
                // A failure of one member is recorded on that member; it must
                // not abandon the members after it.
                let _ = self.hand_off_provider(&mut single).await;
            }
            for member in result["members"].as_array_mut().into_iter().flatten() {
                let id: AgentId = serde_json::from_value(member["agent_id"].clone())?;
                let run = self.resolve_run(&id, None)?;
                member["status"] = json!(run.status.as_str());
            }
        }
        Ok(result)
    }

    /// Renders the public pool admission: stable identities, names, roles and
    /// current statuses only.
    ///
    /// `bound` reflects the durable shared binding stored on the pool row,
    /// not the shape of the request that happened to be replayed, and a
    /// replaced or pruned original member reports `status: null` instead of
    /// failing the whole view.
    fn pool_view(&self, store: &Store, pool: &PoolAdmission) -> Result<Value> {
        let bound: bool = store.conn.query_row(
            "SELECT orchestrator_session_id IS NOT NULL FROM pools WHERE id=?",
            [pool.pool_id.as_str()],
            |row| row.get(0),
        )?;
        let roster_revision: u32 = store.conn.query_row(
            "SELECT roster_revision FROM pools WHERE id=?",
            [pool.pool_id.as_str()],
            |row| row.get(0),
        )?;
        let members = pool
            .members
            .iter()
            .map(|member| {
                let run = self.resolve_run(&member.agent_id, None).ok();
                let mut item = json!({
                    "agent_id":member.agent_id,"name":member.name,"role":member.role,
                    "status":run.map(|row|row.status.as_str()),
                });
                if let Some(enrollment) =
                    agent_run_store::pool_enrollment::view(&store.conn, &member.agent_id)?
                {
                    item["existing"] = json!(true);
                    item["enrollment"] = enrollment;
                }
                Ok(item)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(json!({
            "pool_id": pool.pool_id, "created": pool.created, "bound": bound,
            "roster_revision": roster_revision, "members": members,
        }))
    }

    pub async fn start(&self, mut request: StartRequest) -> Result<Value> {
        request.validate()?;
        if request.runtime == "opencode" {
            return Err(invalid(
                "runtime 'opencode' is no longer supported; remove [runtimes.opencode] from config.toml",
            ));
        }
        let fingerprints = start_replay_fingerprints(&request)?;
        let fingerprint = fingerprints[0].clone();
        {
            let store = Store::open(&self.home)?;
            if let Some(row) = store.replay_request(&request)?
                && let Some(previous) = row
                    .identity
                    .as_ref()
                    .and_then(|v| v.get("replay_request_sha256"))
                    .and_then(Value::as_str)
            {
                if !fingerprints
                    .iter()
                    .any(|fingerprint| fingerprint == previous)
                    || row.parent_agent_id.is_some()
                {
                    return Err(Error::Conflict);
                }
                logging::start(&request.runtime, &request.model, &row.id.to_string(), false);
                return Ok(
                    json!({"agent_id":row.id,"created":false,"agent":self.view(&store,&row)?}),
                );
            }
        }
        if self.system_launcher.is_some() {
            return Err(Error::Unsupported(
                "system execution requires a provider configuration".into(),
            ));
        }
        let config = self.current_config()?;
        let runtime = config.runtime(&request.runtime)?;
        request.account = runtime.selected_account(request.account.as_deref())?;
        let profile = profiles::load(&config, runtime, &request)?;
        request.write = profile.write;
        request.read_roots = profile.read_roots.clone();
        request.required_constraints = profile.required_constraints.clone();
        let role_plan = profile
            .canonical
            .then(|| {
                role_plan::resolve_role_plan(
                    &profile,
                    config.skills_dir(),
                    &config.mcp,
                    if request.account.is_some() {
                        "account"
                    } else {
                        "global"
                    },
                    request.account.as_deref(),
                )
            })
            .transpose()?;
        let config_revision = role_plan
            .as_ref()
            .map(|plan| plan.config_revision.as_str())
            .unwrap_or("pending:materialization");
        adapters::validate(&request, runtime, &profile)?;
        if runtime.kind()? == crate::config::Adapter::Codex {
            crate::codex::Grant::new(runtime, &request, &profile, &self.home)?;
        }
        let policy = policy::evaluate(&request.runtime, runtime, &profile);
        policy.admit()?;
        let identity = LaunchIdentity {
            rust_identity_version: 1,
            replay_request_sha256: Some(fingerprint),
            config: config.clone(),
            profile,
            effective_policy: policy,
            runtime_home: None,
            snapshot_sha256: None,
        };
        let runtime = request.runtime.clone();
        let model = request.model.clone();
        let result = self
            .admit(request, &config, identity, None, config_revision, None)
            .await?;
        if let (Some(agent_id), Some(created)) = (
            result.get("agent_id").and_then(Value::as_str),
            result.get("created").and_then(Value::as_bool),
        ) {
            logging::start(&runtime, &model, agent_id, created);
        }
        Ok(result)
    }
    /// Admits one legacy start request after validating it against the current
    /// configuration, then hands a newly created execution to the detached
    /// supervisor. A replay (`created=false`) launches nothing.
    async fn admit(
        &self,
        request: StartRequest,
        config: &Config,
        identity: LaunchIdentity,
        parent: Option<&Record>,
        config_revision: &str,
        restart_id: Option<i64>,
    ) -> Result<Value> {
        let admitted = self
            .lane("admission")?
            .local(
                agent_run_store::ops::AdmitStart {
                    request: request.clone(),
                    config: config.clone(),
                    config_revision: config_revision.to_owned(),
                    identity: serde_json::to_value(identity)?,
                    parent: parent.map(|row| row.id.clone()),
                    restart_id,
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await?;
        let (id, created) = (admitted.agent_id, !admitted.replayed);
        if created {
            // A failed READY read does not cancel an already admitted durable job.
            let lane = self.lane("admission")?;
            if let Err(error) = supervisor::launch(&self.home, &id, &lane).await {
                let probe = id.clone();
                let row = self.read(move |store| store.get(&probe)).await?;
                lane.event(
                    &id,
                    "supervisor_handoff_error",
                    json!({"kind":error.public().kind}),
                )
                .await?;
                if row.supervisor_pid.is_none() && matches!(error, Error::Io(_)) {
                    // No owned supervisor was established. Explicit failure is better
                    // than an immortal starting row after a failed OS spawn.
                    let out = Outcome::failure("supervisor_bootstrap_failed");
                    lane.finish_terminal(&id, &out).await?;
                }
            }
        }
        let agent = self.view_of(&id).await?;
        Ok(json!({"agent_id":id,"created":created,"agent":agent}))
    }

    /// Runs one read-only store job off the async executor under the bounded
    /// read allowance. A contended database ends the job with a typed busy
    /// error instead of holding an executor thread.
    pub(crate) async fn read<T: Send + 'static>(
        &self,
        job: impl FnOnce(&StoreReader) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let home = self.home.clone();
        tokio::task::spawn_blocking(move || job(&StoreReader::open_with(&home, READ_ALLOWANCE)?))
            .await
            .map_err(|_| Error::Runtime("store read task failed".into()))?
    }

    /// The public view of one execution, read off the executor.
    async fn view_of(&self, id: &AgentId) -> Result<Value> {
        let (service, id) = (self.clone(), id.clone());
        self.read(move |store| service.view(store, &store.get(&id)?))
            .await
    }
    /// Admits one native continuation after proving terminal lineage and snapshot identity.
    ///
    /// The parent must be terminal with a native session and a sealed Rust launch
    /// identity. Snapshot continuations retain the parent's immutable revision;
    /// legacy revisions remain pending until their supervisor rematerializes them.
    /// On a schema-2 home a schema-1 run refuses with
    /// `legacy_continuation_unavailable` and writes nothing; a provider run
    /// continues through [`Self::admit_provider_resume`]. `display_name` inherits
    /// when absent and otherwise replaces the label after normalization; it
    /// participates in request-id replay. The supplied task becomes the next
    /// native turn without a lifetime limit; binding overrides stay validated.
    /// Admission is durable before supervisor handoff; replay launches nothing.
    pub async fn resume(
        &self,
        id: &AgentId,
        task: String,
        request_id: Option<String>,
        display_name: Option<String>,
        orchestrator: Option<OrchestratorRef>,
    ) -> Result<Value> {
        self.resume_for(id, task, request_id, display_name, orchestrator, None)
            .await
    }

    /// Resume an exact terminal execution, optionally as a captured restart member.
    async fn resume_for(
        &self,
        id: &AgentId,
        task: String,
        request_id: Option<String>,
        display_name: Option<String>,
        orchestrator: Option<OrchestratorRef>,
        restart_id: Option<i64>,
    ) -> Result<Value> {
        let parent = Store::open(&self.home)?.get(id)?;
        if !parent.status.terminal() || parent.runtime_session_id.is_none() {
            return Err(invalid(
                "resume requires a terminal run with a native session ID",
            ));
        }
        // On a schema-2 home no continuation is remapped to a provider or
        // replayed as a summary: the history stays readable and the refusal is
        // typed. Explicit provider resume is a separate, later contract.
        let provider_row = parent
            .identity
            .as_ref()
            .is_some_and(|identity| identity["provider_identity_version"] == 2);
        if provider_row {
            let mut result = self
                .admit_provider_resume_for(
                    &parent,
                    task,
                    request_id,
                    display_name,
                    orchestrator,
                    restart_id,
                )
                .await?;
            self.hand_off_provider(&mut result).await?;
            return Ok(result);
        }
        // Replay of the original resume intent precedes every mutable read, as
        // for a provider run: an old parent's retry finds its one child even
        // after later continuations, a changed configuration or a moved home.
        // Exact retries compare task/binding intent without mutable policy or
        // filesystem reads. Execution has no inherited lifetime allowance.
        let mut replay = parent.request.clone();
        replay.task = task.clone();
        replay.request_id = request_id.clone();
        if display_name.is_some() {
            replay.display_name = display_name.clone();
        }
        if orchestrator.is_some() {
            replay.orchestrator = orchestrator.clone();
        }
        replay.validate_intent()?;
        let intent_hash =
            agent_run_domain::canonical::sha256_hex(&json!({"request": replay}), true);
        {
            let store = Store::open(&self.home)?;
            if let Some(child) = store.replay_request(&replay)? {
                if child.parent_agent_id.as_ref() != Some(&parent.id) {
                    return Err(Error::Conflict);
                }
                let frozen_hash = child
                    .identity
                    .as_ref()
                    .and_then(|value| value.get("replay_request_sha256"))
                    .and_then(Value::as_str);
                match frozen_hash {
                    Some(hash)
                        if hash != intent_hash
                            && !store
                                .last_event(&child.id, "historical_execution_policy")?
                                .is_some_and(|policy| policy["version"] == 1) =>
                    {
                        return Err(Error::Conflict);
                    }
                    Some(hash) if hash == intent_hash => {}
                    _ => {
                        // Retired allowances no longer distinguish intent;
                        // every remaining frozen child field still must match.
                        if store.replay_resume_child(&replay, &parent)?.is_none() {
                            return Err(Error::Conflict);
                        }
                    }
                }
                return Ok(
                    json!({"agent_id":child.id,"created":false,"agent":self.view(&store,&child)?}),
                );
            }
        }
        if self.system_launcher.is_some() {
            return Err(Error::Unsupported(
                "system execution cannot resume a legacy run".into(),
            ));
        }
        if matches!(self.active_config()?.value, CachedConfigValue::Providers(_)) {
            return Err(Error::Unsupported(
                "legacy_continuation_unavailable: a schema-1 run cannot be continued under schema 2; its history remains readable".into(),
            ));
        }
        let mut identity = LaunchIdentity::read(&parent)?;
        identity.replay_request_sha256 = Some(intent_hash);
        let runtime_home = identity
            .runtime_home
            .as_deref()
            .ok_or_else(|| invalid("parent has no sealed runtime home"))?;
        let digest = identity
            .snapshot_sha256
            .as_deref()
            .ok_or_else(|| invalid("parent has no runtime snapshot proof"))?;
        adapters::materialize::verify(runtime_home, digest)?;
        let current = self.current_config()?;
        let active = current.runtime(&parent.request.runtime)?;
        if !active.models.contains(&parent.request.model) {
            return Err(invalid("parent model is no longer enabled"));
        }
        let recorded = identity.config.runtime(&parent.request.runtime)?;
        if active.home != recorded.home
            || serde_json::to_value(&active.auth)? != serde_json::to_value(&recorded.auth)?
        {
            return Err(invalid("runtime identity changed since the parent ran"));
        }
        if let Some(account) = parent.request.account.as_deref() {
            active.selected_account(Some(account))?;
        }
        let mut request = parent.request.clone();
        request.task = task;
        request.request_id = request_id;
        // An omitted label inherits the parent's; an explicit one replaces it.
        if display_name.is_some() {
            request.display_name = display_name;
        }
        if orchestrator.is_some() {
            request.orchestrator = orchestrator;
        }
        request.validate()?;
        let config_revision: String = Store::open(&self.home)?.conn.query_row(
            "SELECT config_revision FROM agents WHERE id=?",
            [id.as_str()],
            |row| row.get(0),
        )?;
        let child_revision = if config_revision.starts_with("snapshot:v1:") {
            config_revision.as_str()
        } else {
            "pending:materialization"
        };
        match self
            .admit(
                request,
                &current,
                identity,
                Some(&parent),
                child_revision,
                restart_id,
            )
            .await
        {
            Ok(value) => Ok(value),
            Err(Error::Conflict) => {
                let winner: Option<String> = Store::open(&self.home)?.conn.query_row(
                    "SELECT id FROM agents WHERE parent_agent_id=? ORDER BY sequence DESC LIMIT 1",
                    [id.as_str()],
                    |row| row.get(0),
                ).optional()?;
                match winner {
                    Some(winner) => Err(invalid(format!(
                        "agent {id} has already been resumed by {winner}"
                    ))),
                    None => Err(Error::Conflict),
                }
            }
            Err(error) => Err(error),
        }
    }
    /// Admits an explicit resume of a terminal provider run as a new logical
    /// child, without launching it; [`Self::resume`] hands a created child
    /// to the supervisor.
    ///
    /// The child keeps the parent's provider, harness, explicit model,
    /// workdir, role grants, sealed assets, frozen configuration, runtime
    /// home, completion mode and native session; task text, orchestrator, current global
    /// Codex Fast policy and the per-attempt account lease may change. A request-id replay is answered
    /// before any configuration or quota read. The parent's selection intent
    /// is kept: a pinned run never switches; an automatic run keeps its
    /// previous account while that account is still a valid candidate and
    /// switches only when it is not (disabled, exhausted or no longer bound).
    /// Claude Code keeps session history per login, so it resumes only on the
    /// parent's own account.
    ///
    /// Refuses with `continuation_unavailable` when the sealed assets or the
    /// native history cannot be proved by the continuity checks,
    /// and with a validation error when the current configuration no longer
    /// offers the provider, harness, connection or model. Admission itself
    /// proves the parent terminal, quiescent and cleaned up, and admits at
    /// most one child per parent. `display_name` inherits the frozen label when
    /// absent, or supplies a normalized replacement; a changed label with the
    /// same request id is `Conflict`. Task and orchestrator overrides
    /// remain subject to their normal validation. Returns a durable admission
    /// snapshot; this method does not launch the supervisor.
    pub async fn admit_provider_resume(
        &self,
        parent: &Record,
        task: String,
        request_id: Option<String>,
        display_name: Option<String>,
        orchestrator: Option<OrchestratorRef>,
    ) -> Result<Value> {
        self.admit_provider_resume_for(parent, task, request_id, display_name, orchestrator, None)
            .await
    }

    /// Reuse ordinary provider resume admission for one captured restart member.
    async fn admit_provider_resume_for(
        &self,
        parent: &Record,
        task: String,
        request_id: Option<String>,
        display_name: Option<String>,
        orchestrator: Option<OrchestratorRef>,
        restart_id: Option<i64>,
    ) -> Result<Value> {
        let frozen = ProviderLaunchIdentity::read(parent)?;
        let mut request = frozen.provider_request.clone();
        request.task = task;
        request.request_id = request_id;
        // An omitted label inherits the parent's frozen one; an explicit
        // label replaces it and becomes part of the replay identity.
        if display_name.is_some() {
            request.display_name = display_name;
        }
        if orchestrator.is_some() {
            request.orchestrator = orchestrator;
        } else if let Some(shared) =
            Store::open(&self.home)?.member_pool_binding(&parent.root_agent_id)?
        {
            // A pool member's new execution joins the pool's actual shared
            // binding, which may have been established after the first start.
            request.orchestrator = Some(shared);
        }
        request.validate_intent()?;
        // Replay of the original resume intent precedes every mutable read.
        if let Some(replay) = Store::open(&self.home)?.replay_provider_request(&request)? {
            let store = Store::open(&self.home)?;
            let row = store.get(&replay.agent_id)?;
            if row.parent_agent_id.as_ref() != Some(&parent.id) {
                return Err(Error::Conflict);
            }
            return Ok(
                json!({"agent_id":replay.agent_id,"attempt_id":replay.attempt_id,
                "created":false,"agent":self.view(&store,&row)?}),
            );
        }
        match (&self.system_launcher, &frozen.system_route) {
            (None, None) => {}
            (Some(launcher), Some(route)) => {
                route.validate_for(&frozen.authority.provider, frozen.authority.harness)?;
                if launcher.route(&frozen.authority.provider, frozen.authority.harness)? != *route {
                    return Err(Error::ExecutionChanged);
                }
            }
            _ => {
                return Err(Error::Unsupported(
                    "execution belongs to a different launcher mode".into(),
                ));
            }
        }
        request.validate()?;
        // A parent that already has a child is refused for that reason (the
        // child's own turns legitimately changed the parent's sealed history);
        // the unique parent index remains the final authority.
        let existing: Option<String> = Store::open(&self.home)?
            .conn
            .query_row(
                "SELECT id FROM agents WHERE parent_agent_id=? LIMIT 1",
                [parent.id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(child) = existing {
            return Err(invalid(format!(
                "agent {} has already been resumed by {child}",
                parent.id
            )));
        }
        let session = parent
            .runtime_session_id
            .as_deref()
            .ok_or_else(|| invalid("resume requires a native session ID"))?;
        let runtime_home = frozen.runtime_home.clone().ok_or_else(|| {
            Error::Unsupported("continuation_unavailable: parent has no sealed runtime home".into())
        })?;
        // Resume verifies the parent's own home through the shared-asset
        // registry: a private home keeps the strict verifier, a committed
        // shared layout verifies the same original index through its bound
        // trees, and a still-prepared row refuses — the parent's history is
        // never rebuilt from changed live sources.
        let preflight_digest =
            if let (Some(launcher), Some(route)) = (&self.system_launcher, &frozen.system_route) {
                // Bind the worker's read-only proof to the same parent snapshot
                // that admission will use; the writer compares it again in its
                // transaction after replay and lineage checks.
                let expected = {
                    let store = Store::open(&self.home)?;
                    store.conn.execute_batch("BEGIN")?;
                    let current = store.get(&parent.id)?;
                    if current.identity != parent.identity
                        || current.request != parent.request
                        || current.runtime_session_id != parent.runtime_session_id
                        || current.status != parent.status
                    {
                        return Err(Error::ExecutionChanged);
                    }
                    agent_run_store::provider_admission::resume_preflight_digest(
                        &store.conn,
                        &parent.id,
                    )?
                };
                let actual = launcher
                    .preflight_resume(parent.id.clone(), route.clone())
                    .await?;
                if actual != expected {
                    return Err(Error::ExecutionChanged);
                }
                Some(expected)
            } else {
                crate::runtime_storage::verify(
                    &Store::open(&self.home)?,
                    &self.home,
                    &runtime_home,
                    frozen.authority.assets_sha256.as_str(),
                )
                .map_err(|_| {
                    Error::Unsupported(
                        "continuation_unavailable: parent runtime assets no longer verify".into(),
                    )
                })?;
                None
            };
        let (prefer, intent, pinned_id, adapter_state): (String, String, Option<String>, String) =
            Store::open(&self.home)?.conn.query_row(
                "SELECT t.selected_account_id,a.selection_intent,a.requested_account_id,t.adapter_state_json \
                 FROM attempts t JOIN agents a ON a.id=t.agent_id \
                 WHERE t.agent_id=? ORDER BY t.number DESC LIMIT 1",
                [parent.id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
        let prefer: agent_run_domain::catalog::AccountId = prefer.parse()?;
        // The current configuration may only narrow the frozen authority.
        let (current, _) = self.current_provider_config()?;
        let accounts = Store::open(&self.home)?.list_accounts()?;
        let now_catalog = current.resolve_catalog(accounts.clone())?;
        let authority = &frozen.authority;
        let offered = now_catalog
            .provider(&authority.provider)
            .filter(|definition| {
                definition.harness == authority.harness
                    && definition.connection == authority.connection
                    && definition
                        .models
                        .iter()
                        .any(|model| model.id == authority.model)
            });
        let Some(offered) = offered else {
            return Err(invalid(
                "provider, harness, connection or model changed since the parent ran; resume refused",
            ));
        };
        // Current policy must still permit the frozen execution; it is never
        // replaced by the current one. Checked before the history proof so a
        // policy refusal is reported as such.
        current_policy_permits(&current, &frozen, &request)?;
        // Only the seal recorded at the parent's cleanup boundary is trusted.
        // At admission only Codex's root is known (its run home); the
        // handoff binds every harness to the launch plan's actual root.
        let expected_root = (authority.harness == agent_run_domain::catalog::HarnessId::Codex)
            .then_some(runtime_home.as_path());
        if preflight_digest.is_none() {
            verify_recorded_history(&adapter_state, &frozen, expected_root, session)?;
        }
        let bound: std::collections::BTreeSet<_> = offered
            .bindings
            .iter()
            .filter(|binding| {
                binding
                    .models
                    .as_ref()
                    .is_none_or(|models| models.contains(&authority.model))
            })
            .map(|binding| binding.account.clone())
            .collect();
        let mut child_authority = authority.clone();
        let ordered_claude = authority.harness == agent_run_domain::catalog::HarnessId::ClaudeCode
            && frozen
                .provider_config
                .providers
                .get(&authority.provider)
                .is_some_and(|provider| provider.ordered_accounts);
        child_authority.eligible_accounts.retain(|account| {
            bound.contains(account)
                && (authority.harness == agent_run_domain::catalog::HarnessId::Codex
                    || ordered_claude
                    || *account == prefer)
        });
        let frozen_catalog = frozen.provider_config.resolve_catalog(accounts)?;
        let hard: std::collections::BTreeSet<_> = frozen_catalog
            .provider(&authority.provider)
            .map(|definition| {
                definition
                    .bindings
                    .iter()
                    .map(|binding| binding.account.clone())
                    .filter(|account| !child_authority.eligible_accounts.contains(account))
                    .collect()
            })
            .unwrap_or_default();
        let pinned = match (intent.as_str(), pinned_id) {
            ("pinned", Some(id)) => Some(id.parse::<agent_run_domain::catalog::AccountId>()?),
            ("auto", None) => None,
            _ => {
                return Err(Error::Integrity(
                    "parent selection intent is malformed".into(),
                ));
            }
        };
        let mut effective = parent.request.clone();
        effective.task = request.task.clone();
        effective.request_id = request.request_id.clone();
        effective.display_name = request.display_name.clone();
        effective.orchestrator = request.orchestrator.clone();
        // Preserve raw replay intent while each resume captures current global Fast policy.
        effective.fast = request.fast
            || (authority.harness == agent_run_domain::HarnessId::Codex
                && current
                    .harnesses
                    .get(&authority.harness)
                    .is_some_and(|harness| harness.fast_mode));
        let identity = serde_json::to_value(ProviderLaunchIdentity {
            provider_identity_version: 2,
            replay_request_sha256: agent_run_domain::canonical::sha256_hex(
                &serde_json::to_value(&request)?,
                true,
            ),
            provider_request: request.clone(),
            provider_config_sha256: frozen.provider_config_sha256.clone(),
            provider_config: frozen.provider_config.clone(),
            provider_config_snapshot: frozen.provider_config_snapshot.clone(),
            authority: child_authority.clone(),
            system_route: frozen.system_route.clone(),
            runtime_home: Some(runtime_home),
            snapshot_sha256: frozen.snapshot_sha256.clone(),
        })?;
        let cap = current
            .harnesses
            .get(&authority.harness)
            .ok_or_else(|| invalid("provider harness is not configured"))?
            .max_active_agents;
        let pin_label = request
            .account
            .as_ref()
            .map(|label| label.as_str().to_owned());
        let mut submission = 0;
        let mut decisions = crate::capacity::quota_adapter::DecisionSnapshots::default();
        let admission = loop {
            let snapshot = decisions.read(current.capacity.quota.as_ref()).await;
            let ranked_catalog = frozen_catalog.clone();
            let provider = authority.provider.clone();
            let model = authority.model.clone();
            let pin = pin_label.clone();
            let hard = hard.clone();
            let candidates = self
                .read(move |store| {
                    Ok(crate::capacity::quota_adapter::candidates_at(
                        store,
                        &ranked_catalog,
                        &provider,
                        &model,
                        pin.as_deref(),
                        &hard,
                        snapshot.as_deref(),
                        now(),
                    )?
                    .candidates)
                })
                .await?;
            match self
                .lane("admission")?
                .local(
                    agent_run_store::ops::AdmitProviderResume {
                        catalog: frozen_catalog.clone(),
                        parent: parent.id.clone(),
                        prefer: prefer.clone(),
                        preflight_digest: preflight_digest.clone(),
                        restart_id,
                        inputs: agent_run_store::ops::AdmissionSpec {
                            request: request.clone(),
                            effective: effective.clone(),
                            authority: child_authority.clone(),
                            candidates,
                            identity: identity.clone(),
                            global_cap: current.core.max_active_agents,
                            harness_cap: cap,
                            pinned: pinned.clone(),
                        },
                    },
                    crate::broker_writer::BROKER_BUDGET,
                )
                .await
            {
                Err(Error::QuotaAdmission(
                    QuotaAdmissionError::QuotaClockRollback
                    | QuotaAdmissionError::QuotaDecisionInvalid,
                )) if decisions.retry_invalidated(&mut submission, PROVIDER_STALE_RETRIES) => {}
                Err(Error::QuotaAdmission(QuotaAdmissionError::QuotaDecisionStale))
                    if decisions.retry_expired(&mut submission, PROVIDER_STALE_RETRIES) => {}
                Err(Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. }))
                    if submission < PROVIDER_STALE_RETRIES =>
                {
                    submission += 1;
                    decisions.require_fresh();
                }
                Err(Error::QuotaAdmission(QuotaAdmissionError::SelectionStale { .. })) => {
                    return Err(QuotaAdmissionError::SelectionBusy {
                        stale_retries: PROVIDER_STALE_RETRIES,
                    }
                    .into());
                }
                result => break result?,
            }
        };
        let store = Store::open(&self.home)?;
        let row = store.get(&admission.agent_id)?;
        Ok(
            json!({"agent_id":admission.agent_id,"attempt_id":admission.attempt_id,
            "created":admission.created,"agent":self.view(&store,&row)?}),
        )
    }

    /// Continue one captured agent after its restart deadline is recorded.
    pub(crate) async fn resume_service_restart(
        &self,
        restart_id: i64,
        id: &AgentId,
    ) -> Result<Value> {
        let result = self
            .resume_for(
                id,
                "Agent Run has finished its restart procedure. Continue your existing work from where you stopped."
                    .into(),
                Some(format!("service-restart:{restart_id}:{id}")),
                None,
                None,
                Some(restart_id),
            )
            .await;
        if let Err(error) = &result
            && !matches!(
                error,
                Error::Persistence { .. } | Error::WriterFailure(_) | Error::BrokerUnavailable
            )
        {
            let reason = match error {
                Error::Unsupported(message)
                    if message.starts_with("continuation_unavailable:")
                        && message.len() <= 180
                        && message.bytes().all(|byte| {
                            byte.is_ascii_alphanumeric() || b" _:-.".contains(&byte)
                        }) =>
                {
                    message.clone()
                }
                Error::ExecutionChanged => "The original execution or account changed.".into(),
                Error::Validation(message)
                    if message == "resume requires a terminal run with a native session ID" =>
                {
                    "The native session is unavailable.".into()
                }
                _ => format!("Resume was refused: {}", error.public().kind),
            };
            let _ = self
                .lane("service-restart")?
                .local(
                    agent_run_store::ops::FailServiceRestartResume {
                        restart_id,
                        agent_id: id.clone(),
                        reason,
                    },
                    crate::broker_writer::BROKER_BUDGET,
                )
                .await;
        }
        result
    }

    /// Start the deadline before a captured agent's resume is dispatched.
    pub(crate) async fn request_service_restart_resume(
        &self,
        restart_id: i64,
        id: &AgentId,
    ) -> Result<()> {
        let (boot_id, now_ns) = agent_run_platform::monotonic::now()?;
        self.lane("service-restart")?
            .local(
                agent_run_store::ops::RequestServiceRestartResume {
                    restart_id,
                    agent_id: id.clone(),
                    boot_id,
                    now_ns,
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await
    }

    /// Reconcile confirmed resumes and the configured 60-second deadline.
    pub(crate) async fn refresh_service_restart(&self, restart_id: i64) -> Result<bool> {
        let (boot_id, now_ns) = agent_run_platform::monotonic::now()?;
        self.lane("service-restart")?
            .local(
                agent_run_store::ops::RefreshServiceRestart {
                    restart_id,
                    boot_id,
                    now_ns,
                    at: now(),
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await
    }

    /// Enqueues one durable cancellation and returns the agent's public view.
    ///
    /// The pending command is persisted exactly as [`Store::enqueue`] records
    /// it, so command durability is unchanged; the returned envelope is the
    /// current agent view including its top-level `status`, matching the
    /// archived Python `self.get(agent_id)` response.  An unknown or already
    /// terminal agent fails before any command is queued.
    pub async fn cancel(&self, id: &AgentId) -> Result<Value> {
        self.lane("commands")?
            .remote(
                agent_run_store::ops::EnqueueCommand {
                    execution: id.clone(),
                    kind: "cancel".into(),
                    payload: json!({}),
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await?;
        self.view_of(id).await
    }
    /// Enqueues one nonblank bounded steering message for an active agent.
    ///
    /// `text` may contain arbitrary UTF-8 up to 256 KiB. The command is
    /// persisted before this method returns; unknown agents, invalid text, and
    /// storage failures are returned without contacting the runtime directly.
    pub async fn steer(&self, id: &AgentId, text: &str) -> Result<Value> {
        crate::domain::nonblank("steer text", text)?;
        if text.len() > 256 * 1024 {
            return Err(invalid("steer text exceeds 256 KiB"));
        }
        self.lane("commands")?
            .remote(
                agent_run_store::ops::EnqueueCommand {
                    execution: id.clone(),
                    kind: "steer".into(),
                    payload: json!({"text":text}),
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await
    }
    /// Reads a pinned execution's answer using the shared sealed-proof verifier.
    /// Unavailable answers retain their metadata envelope; verified inline text
    /// keeps the historical 1 MiB CLI cutoff independently of transport budgets.
    pub fn answer(&self, id: &AgentId) -> Result<Value> {
        let store = Store::open(&self.home)?;
        let row = store.get(id)?;
        let restart = agent_run_store::service_restart::restart_view(&store.conn, id)?;
        let status = match restart.as_ref().map(|(status, _)| status.as_str()) {
            Some("failed") => "failed",
            Some("cancelled") => "cancelled",
            Some("cancelling") => "cancelling",
            Some(_) => "running",
            None => row.status.as_str(),
        };
        let Some(path) = &row.answer_path else {
            return Ok(
                json!({"agent_id":id,"status":status,"available":false,"path":null,"size_bytes":null,"sha256":null,"content":null,"inline_complete":false,"relative_path":null,"kind":null,"media_type":null,"proof_version":null}),
            );
        };
        let proof = crate::observation::answer_proof(&row)?;
        let root = self.home.join("agents").join(id.as_str());
        let (version, content) = verify::read(&root, &proof, verify::INLINE_ANSWER)?;
        Ok(
            json!({"agent_id":id,"status":row.status,"available":true,"path":path,"size_bytes":proof.bytes,"sha256":proof.sha256,"inline_complete":content.is_some(),"content":content,"relative_path":path.strip_prefix(&root).ok(),"kind":"agent_answer","media_type":verify::MEDIA_TYPE,"proof_version":version}),
        )
    }
    pub fn transcript(&self, id: &AgentId, cursor: i64, limit: usize) -> Result<Value> {
        Store::open(&self.home)?.transcript(id, cursor, limit)
    }
    pub fn delivery_status(&self, id: &AgentId) -> Result<Value> {
        Store::open(&self.home)?.delivery_status(id)
    }
    /// Cancels one pending completion-delivery notification by durable identifier.
    ///
    /// A missing, delivered, or already cancelled notification returns `false` in
    /// the Python-compatible acknowledgement; invalid identifiers are rejected.
    pub async fn delivery_cancel(&self, delivery_id: &str) -> Result<Value> {
        let cancelled = self
            .lane("delivery")?
            .remote(
                agent_run_store::ops::CancelDelivery {
                    delivery_id: delivery_id.to_owned(),
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await?;
        Ok(json!({"delivery_id":delivery_id,"cancelled":cancelled}))
    }

    /// Lists or idempotently releases operator history pins without worker authority.
    pub async fn history_pins(
        &self,
        request: agent_run_domain::batch::HistoryPinsRequest,
    ) -> Result<Value> {
        use agent_run_domain::batch::HistoryPinsRequest;
        match request {
            HistoryPinsRequest::Release {
                pin_id,
                expected_store_id,
            } => {
                let result = self
                    .lane("retention")?
                    .local(
                        agent_run_store::ops::ReleaseHistoryPin {
                            pin_id: pin_id.clone(),
                            expected_store_id,
                        },
                        crate::broker_writer::BROKER_BUDGET,
                    )
                    .await?
                    .ok_or_else(|| Error::PinNotFound(pin_id.to_string()))?;
                Ok(serde_json::to_value(result)?)
            }
            HistoryPinsRequest::List {
                status,
                limit,
                after_pin_id,
                expected_store_id,
            } => {
                let home = self.home.clone();
                tokio::task::spawn_blocking(move || {
                    let store = Store::open(&home)?;
                    Ok(serde_json::to_value(store.history_pins_scoped(
                        status,
                        limit,
                        after_pin_id.as_ref(),
                        expected_store_id.as_ref(),
                    )?)?)
                })
                .await
                .map_err(|_| Error::Runtime("history pins read failed".into()))?
            }
        }
    }
    /// List execution history, optionally waiting for a newer durable revision.
    pub async fn list(&self, query: Query) -> Result<Value> {
        self.list_selected(query, false).await
    }

    /// Share revision waiting and pagination with the stable public agent list.
    pub(crate) async fn list_selected(&self, query: Query, latest: bool) -> Result<Value> {
        query.validate()?;
        let started = tokio::time::Instant::now();
        let until = started + Duration::from_secs_f64(query.wait_seconds);
        loop {
            let value = {
                let store = Store::open(&self.home)?;
                let revision = store.revision()?;
                let message_revision = store.message_revision()?;
                // Journal rows never advance the event revision, so a
                // transcript watermark explicitly opts into waking on
                // transcript and tool-count progress. Pure journal wakes are
                // paced: they take effect at most one second after the wait
                // began, which bounds snapshot rebuilds to once per second
                // per follower during heavy streaming, while event wakes keep
                // their immediate historical behavior.
                let events_advanced = query.after_revision.is_none_or(|r| revision > r);
                let journal_advanced = query
                    .after_message_revision
                    .is_some_and(|m| message_revision > m)
                    && started.elapsed() >= Duration::from_secs(1);
                if tokio::time::Instant::now() < until && !events_advanced && !journal_advanced {
                    None
                } else {
                    let list = if latest {
                        Store::list_latest
                    } else {
                        Store::list
                    };
                    let (rows, total) = list(
                        &store,
                        query.active,
                        query.offset,
                        query.limit,
                        query.orchestrator.as_ref(),
                    )?;
                    let items = rows
                        .iter()
                        .map(|r| self.view(&store, r))
                        .collect::<Result<Vec<_>>>()?;
                    let next = query.offset.saturating_add(items.len());
                    Some(
                        json!({"items":items,"total":total,"offset":query.offset,"limit":query.limit,"next_offset":if next<total as usize{Some(next)}else{None},"complete":next>=total as usize,"revision":revision,"message_revision":message_revision,"observed_at":now()}),
                    )
                }
            };
            if let Some(value) = value {
                return Ok(value);
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }
    /// Wait for a terminal durable revision or this observer's optional deadline.
    /// A service-restart continuation follows its confirmed successor; ordinary
    /// user-requested continuations remain pinned to the selected execution.
    ///
    /// The run itself is never given an execution deadline here: `seconds`
    /// bounds only this observer.  Each read opens and drops its own
    /// thread-affine store connection before the Tokio sleep, allowing another
    /// process to commit the terminal status that supplies the answer envelope.
    pub async fn wait(&self, id: &AgentId, seconds: Option<f64>) -> Result<Value> {
        if seconds.is_some_and(|n| !n.is_finite() || n < 0.0) {
            return Err(invalid("wait_seconds must be finite and nonnegative"));
        }
        let until = seconds
            .map(|s| {
                let duration = Duration::try_from_secs_f64(s)
                    .map_err(|_| invalid("wait_seconds is too large"))?;
                tokio::time::Instant::now()
                    .checked_add(duration)
                    .ok_or_else(|| invalid("wait deadline is out of range"))
            })
            .transpose()?;
        let mut current = id.clone();
        loop {
            // This scope drops the thread-affine connection before the await.
            // A waiter therefore owns neither a SQLite transaction nor a socket
            // lane while another process commits the terminal revision.
            let (row, restart, continuation) = {
                let store = Store::open(&self.home)?;
                let row = store.get(&current)?;
                let restart =
                    agent_run_store::service_restart::restart_view(&store.conn, &current)?;
                let continuation: Option<Option<String>> = store.conn.query_row(
                    "SELECT resumed_execution_id FROM service_restart_agents WHERE agent_id=? AND resume_status IN ('started','cancelled') AND resumed_execution_id IS NOT NULL ORDER BY restart_id DESC LIMIT 1",
                    [current.as_str()], |row| row.get(0),
                ).optional()?;
                (row, restart, continuation.flatten())
            };
            if let Some(child) = continuation {
                current = child.parse()?;
                continue;
            }
            let restarting = restart.as_ref().is_some_and(|(state, _)| {
                matches!(state.as_str(), "pending" | "unconfirmed" | "cancelling")
            });
            if row.status.terminal() && !restarting {
                return self.answer(&current);
            }
            if until.is_some_and(|d| tokio::time::Instant::now() >= d) {
                return Ok(
                    json!({"agent_id":id,"status":if restarting {"running"} else {row.status.as_str()},"terminal":false,"available":false}),
                );
            }
            let poll = Duration::from_millis(200);
            let sleep = until
                .map(|deadline| {
                    deadline
                        .saturating_duration_since(tokio::time::Instant::now())
                        .min(poll)
                })
                .unwrap_or(poll);
            tokio::time::sleep(sleep).await;
        }
    }
    /// Projects the exact execution `row` using committed evidence in `store`.
    /// Includes the admitted human name, nullable recorded native usage and
    /// complete lineage totals, plus lifecycle, delivery and process observations
    /// at the current UTC time. Missing measurements remain null. No state is
    /// written and no process is launched; store/serialization errors propagate.
    /// Public transports subsequently normalize execution identity to the root.
    pub fn view(&self, store: &Store, row: &Record) -> Result<Value> {
        let observed = now();
        let progress = store.last_progress(&row.id)?;
        let phase = if row.status.terminal() {
            "terminal"
        } else if row.status == Status::Running {
            "running"
        } else if row.status == Status::Cancelling {
            "stopping"
        } else {
            "accepted"
        };
        let phase_event = store.last_event(&row.id, "phase")?;
        let phase = if phase == "accepted" {
            phase_event
                .as_ref()
                .and_then(|v| v.get("phase"))
                .and_then(Value::as_str)
                .filter(|s| ["preparing", "spawning"].contains(s))
                .unwrap_or(phase)
        } else {
            phase
        };
        let policy = row
            .identity
            .as_ref()
            .and_then(|i| i.get("effective_policy"));
        let mut view = json!({"agent_id":row.id,"name":row.display_name,"runtime":row.request.runtime,"model":row.request.model,"profile":row.request.profile,"task_summary":row.request.task.split_whitespace().collect::<Vec<_>>().join(" ").chars().take(160).collect::<String>(),"status":row.status,
            "created_at":row.created_at,"started_at":row.started_at,"finished_at":row.finished_at,"elapsed_seconds":(row.finished_at.unwrap_or(observed)-row.started_at.unwrap_or(row.created_at)).max(0.0),"last_progress_at":progress,"silence_seconds":if row.status.terminal(){None}else{Some((observed-progress.or(row.started_at).unwrap_or(row.created_at)).max(0.0))},"failure_kind":row.failure_kind,"failure_text":row.failure_text,"answer_available":row.answer_path.is_some(),"answer_bytes":row.answer_bytes,"answer_sha256":row.answer_sha256,"effort":row.request.effort,
            "delivery":store.delivery_status(&row.id)?,"parent_agent_id":row.parent_agent_id,"root_agent_id":row.root_agent_id,"sequence":row.sequence,"cleanup":store.last_event(&row.id,"process_cleanup")?,"policy":policy,"phase":phase,"phase_started_at":row.finished_at.or(row.started_at).unwrap_or(row.created_at),"process_state":process::observe(row.supervisor_pid,row.supervisor_identity.as_deref(),row.supervisor_birth_time),"observed_at":observed,"runtime_outcome":if row.status.terminal(){Some(row.status.as_str())}else{None},"acceptance":"pending","workdir":row.request.workdir.display().to_string(),
            "usage":store.usage_view(&row.id)?,"usage_cumulative":store.usage_cumulative(&row.root_agent_id)?,"tool_counts":store.tool_counts(&row.id)?});
        if let Some((status, reason)) =
            agent_run_store::service_restart::restart_view(&store.conn, &row.id)?
        {
            let failed = status == "failed";
            let cancelled = status == "cancelled";
            let cancelling = status == "cancelling";
            view["status"] = json!(if failed {
                "failed"
            } else if cancelled {
                "cancelled"
            } else if cancelling {
                "cancelling"
            } else {
                "running"
            });
            view["phase"] = json!(match status.as_str() {
                "failed" | "cancelled" => "terminal",
                "cancelling" => "stopping",
                "unconfirmed" => "restart_unconfirmed",
                _ => "restarting",
            });
            if !failed && !cancelled {
                view["finished_at"] = Value::Null;
            }
            view["runtime_outcome"] = if failed {
                json!("failed")
            } else if cancelled {
                json!("cancelled")
            } else {
                Value::Null
            };
            view["answer_available"] = json!(false);
            view["answer_bytes"] = Value::Null;
            view["answer_sha256"] = Value::Null;
            view["failure_kind"] = if failed {
                json!("restart_resume_failed")
            } else {
                Value::Null
            };
            view["failure_text"] = serde_json::to_value(reason)?;
        }
        if let Some(lifecycle) = store.worker_lifecycle_view_at(&row.id, observed)? {
            if !row.status.terminal() && row.status == Status::Running {
                view["phase"] = lifecycle["phase"].clone();
                view["phase_started_at"] = lifecycle["transition_at"].clone();
            }
            view["completion_mode"] = json!("explicit_finish");
            view["turn_count"] = lifecycle["turn_count"].clone();
            view["idle_seconds"] = lifecycle["idle_seconds"].clone();
        }
        if let Some(health) = store.health_view(&row.id)? {
            view["health"] = health;
        }
        let mcp = agent_run_store::projections::selected_mcp(row.identity.as_ref());
        if !mcp.is_empty() {
            view["mcp"] = serde_json::to_value(mcp)?;
        }
        Ok(view)
    }
    /// Reconcile a bounded fair page of rows whose recorded ownership is proven gone.
    ///
    /// Native process observation is performed by the lifecycle module and
    /// never treats unavailable OS evidence as a terminal outcome.
    /// Pool eligibility is read off the executor before admission; unchanged
    /// pools create no settlement receipts. The writer rechecks each decision.
    pub async fn reconcile(&self) -> Result<usize> {
        let lane = self.lane("reconcile")?;
        let reconciled = reconcile::queued::reconcile(&self.home, &lane, 100)
            .await?
            .len();
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs() as usize);
        if self
            .read(move |reader| reader.pool_settlement_pending(POOL_SWEEP_LIMIT, seed))
            .await?
        {
            lane.local(
                agent_run_store::ops::SettlePool {
                    target: agent_run_store::ops::SettleTarget::Open {
                        limit: POOL_SWEEP_LIMIT,
                        seed,
                    },
                },
                crate::broker_writer::BROKER_BUDGET,
            )
            .await?;
        }
        Ok(reconciled)
    }
    /// Returns the active cached config revision after the request-boundary
    /// digest check; an invalid edit keeps the last valid revision active.
    fn active_config(&self) -> Result<CachedConfig> {
        self.refresh_config()?;
        self.config
            .read()
            .map_err(|_| Error::Runtime("configuration cache lock is poisoned".into()))?
            .clone()
            .ok_or_else(|| Error::Runtime("configuration cache is empty".into()))
    }

    /// Public `models` read.
    ///
    /// Schema 2 combines one fresh Quota response with one local read snapshot;
    /// unavailable quota is unknown. Schema 1 keeps its runtime roster.
    pub async fn models(&self, query: agent_run_domain::ModelsQuery) -> Result<Value> {
        let active = self.active_config()?;
        match active.value {
            CachedConfigValue::Providers(config) => {
                let snapshot =
                    crate::capacity::quota_adapter::fetch(config.capacity.quota.as_ref()).await;
                let home = self.home.clone();
                self.read(move |store| {
                    crate::capacity::quota_projection::models_at(
                        store,
                        &home,
                        &config,
                        &active.revision,
                        &query,
                        snapshot.as_ref(),
                        now(),
                    )
                })
                .await
            }
            CachedConfigValue::Legacy(_) if query.is_empty() => {
                crate::capacity::models(&self.home).await
            }
            CachedConfigValue::Legacy(_) => Err(Error::Unsupported(
                "models filters require schema_version 2".into(),
            )),
        }
    }
    /// Projects Quota windows and ranking with explicit completeness. Schema 1
    /// requires migration to schema 2; no collector or engine is launched.
    pub async fn limits(&self) -> Result<Value> {
        let active = self.active_config()?;
        match active.value {
            CachedConfigValue::Providers(config) => {
                let snapshot =
                    crate::capacity::quota_adapter::fetch(config.capacity.quota.as_ref()).await;
                self.read(move |store| {
                    crate::capacity::quota_projection::limits_at(
                        store,
                        &config,
                        &active.revision,
                        snapshot.as_ref(),
                        now(),
                    )
                })
                .await
            }
            CachedConfigValue::Legacy(_) => {
                crate::capacity::quota_projection::legacy_limits_unavailable()
            }
        }
    }

    /// Returns provider order over one Quota response and local snapshot.
    /// Schema 1 requires an explicit migration to schema 2.
    pub async fn capacity_order(
        &self,
        query: agent_run_domain::CapacityOrderQuery,
    ) -> Result<Value> {
        let active = self.active_config()?;
        match active.value {
            CachedConfigValue::Providers(config) => {
                let snapshot =
                    crate::capacity::quota_adapter::fetch(config.capacity.quota.as_ref()).await;
                self.read(move |store| {
                    crate::capacity::quota_projection::order_at(
                        store,
                        &config,
                        &active.revision,
                        &query,
                        snapshot.as_ref(),
                        now(),
                    )
                })
                .await
            }
            CachedConfigValue::Legacy(_) => {
                crate::capacity::quota_projection::legacy_order_unavailable()
            }
        }
    }

    /// Public `delegation_guide` read: compact routing text over the same
    /// single committed snapshot [`Service::models`] would return for the
    /// default query, rendered by [`crate::delegation_guide::render`] without
    /// any new quota read, collection, or ranking. A schema-1 config has no
    /// provider catalog, so the whole read is `Unsupported` there.
    pub async fn delegation_guide(&self) -> Result<Value> {
        self.delegation_guide_filtered(agent_run_domain::ModelsQuery::default())
            .await
    }

    /// Renders the same compact account-free guidance for exact provider/model/
    /// profile filters, using the catalog's admission rules and one committed
    /// snapshot. Unknown filters are typed ValidationError; schema 1 remains
    /// Unsupported. Omitted filters retain the historical default guidance.
    pub async fn delegation_guide_filtered(
        &self,
        query: agent_run_domain::ModelsQuery,
    ) -> Result<Value> {
        let active = self.active_config()?;
        match active.value {
            CachedConfigValue::Providers(config) => {
                let snapshot =
                    crate::capacity::quota_adapter::fetch(config.capacity.quota.as_ref()).await;
                let home = self.home.clone();
                self.read(move |store| {
                    crate::capacity::quota_projection::delegation_guide_at(
                        store,
                        &home,
                        &config,
                        &active.revision,
                        &query,
                        snapshot.as_ref(),
                        now(),
                    )
                })
                .await
            }
            CachedConfigValue::Legacy(_) => Err(Error::Unsupported(
                "delegation_guide requires schema_version 2".into(),
            )),
        }
    }
}

/// Identifies a possibly committed writer operation or unreadable committed result.
fn batch_admission_unknown(error: &Error) -> bool {
    matches!(error, Error::Json(_))
        || matches!(error,
            Error::Persistence { source, .. }
                if matches!(source.as_ref(), Error::WriterFailure(agent_run_domain::storage::WriterFailure::Unknown(_)))
        )
}

/// Best-effort, payload-free logging must not stop later independent handoffs.
fn batch_handoff_log(id: &AgentId, class: &str) {
    let _ = std::panic::catch_unwind(|| {
        if let Some(logger) = logging::configured() {
            logger.log(
                logging::Level::Warning,
                &format!("batch_handoff_unavailable agent_id={id} class={class}"),
            );
        }
    });
}

/// Refuses a provider resume when the current configuration no longer
/// permits the parent's frozen execution: the provider no longer runs the
/// frozen harness through the frozen connection, the offering's native model
/// alias changed, its configured effort choices exclude the frozen effective effort, a
/// current hard model restriction is absent from the frozen role,
/// the current canonical role no longer grants something the frozen role
/// used (write, network, external read roots, a read root, a skill or MCP
/// server/tool cap) or requires a constraint the frozen role lacks, or the current
/// harness policy rejects the frozen grants. Advice and ranking weights are
/// not compared. The frozen authority is never replaced by the current one.
pub(crate) fn current_policy_permits(
    current: &ProviderConfig,
    frozen: &ProviderLaunchIdentity,
    request: &ProviderStartRequest,
) -> Result<()> {
    let authority = &frozen.authority;
    let refuse = |what: &str| {
        Err(invalid(format!(
            "current policy no longer permits the parent's frozen execution ({what}); resume refused"
        )))
    };
    let offering = |config: &ProviderConfig| {
        config
            .providers
            .get(&authority.provider)
            .and_then(|provider| {
                provider
                    .models
                    .iter()
                    .find(|model| model.id == authority.model)
            })
            .cloned()
    };
    let (Some(now), Some(then)) = (offering(current), offering(&frozen.provider_config)) else {
        return refuse("model offering");
    };
    // The one offer identity guard shared by explicit resume, automatic
    // allocation and the switch handoff: the current provider must still
    // run the frozen harness through the frozen connection (endpoint,
    // protocol, header style), never a different one under the same id.
    if current
        .providers
        .get(&authority.provider)
        .is_none_or(|settings| {
            settings.harness != authority.harness || settings.connection != authority.connection
        })
    {
        return refuse("harness or connection");
    }
    if frozen
        .provider_config
        .providers
        .get(&authority.provider)
        .is_some_and(|provider| provider.ordered_accounts)
        && current
            .providers
            .get(&authority.provider)
            .is_none_or(|provider| !provider.ordered_accounts)
    {
        return refuse("account queue");
    }
    if now.native_model.as_deref().unwrap_or(&now.id)
        != then.native_model.as_deref().unwrap_or(&then.id)
    {
        return refuse("native model alias");
    }
    if now.permits_effort(authority.effort.as_deref()).is_err() {
        return refuse("effort");
    }
    let role =
        agent_run_config::role_plan::ResolvedRolePlan::from_payload(&authority.role_payload)?;
    if !now
        .restrictions
        .iter()
        .all(|restriction| role.required_constraints.contains(restriction))
    {
        return refuse("model restrictions");
    }
    let Ok(mut profile) = profiles::load_provider(current, request) else {
        return refuse("canonical role");
    };
    profile
        .required_constraints
        .extend(now.restrictions.iter().copied());
    let Ok(now_role) = role_plan::resolve_role_plan(
        &profile,
        current.skills_dir(),
        &current.mcp,
        if request.account.is_some() {
            "account"
        } else {
            "global"
        },
        request.account.as_ref().map(|label| label.as_str()),
    ) else {
        return refuse("role assets");
    };
    let ids = |items: &[String]| {
        items
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
    };
    let frozen_skills = ids(&role
        .skills
        .iter()
        .map(|skill| skill.id.clone())
        .collect::<Vec<_>>());
    let frozen_mcp = ids(&role
        .mcp
        .iter()
        .map(|server| server.id.clone())
        .collect::<Vec<_>>());
    let now_skills = ids(&now_role
        .skills
        .iter()
        .map(|skill| skill.id.clone())
        .collect::<Vec<_>>());
    let now_mcp = ids(&now_role
        .mcp
        .iter()
        .map(|server| server.id.clone())
        .collect::<Vec<_>>());
    if (role.write && !now_role.write)
        || (role.network && !now_role.network)
        || (role.allow_external_read_roots && !now_role.allow_external_read_roots)
        || !role
            .read_roots
            .iter()
            .all(|root| now_role.read_roots.contains(root))
        || !frozen_skills.is_subset(&now_skills)
        || !frozen_mcp.is_subset(&now_mcp)
        || !role.mcp.iter().all(|server| {
            now_role.mcp.iter().any(|current| {
                current.id == server.id
                    && match (&server.allowed_tools, &current.allowed_tools) {
                        (_, None) => true,
                        (Some(frozen), Some(now)) => frozen.iter().all(|tool| now.contains(tool)),
                        (None, Some(_)) => false,
                    }
            })
        })
        || !now_role
            .required_constraints
            .is_subset(&role.required_constraints)
    {
        return refuse("role grants");
    }
    let runtime = adapters::provider::runtime(current, authority.harness, &authority.model)?;
    let frozen_profile = Profile {
        name: role.role_name.clone(),
        body: role.prompt.clone(),
        write: role.write,
        network: role.network,
        revision: role.role_revision.clone(),
        canonical: true,
        allow_external_read_roots: role.allow_external_read_roots,
        read_roots: role.read_roots.clone(),
        skills: frozen_skills.into_iter().collect(),
        mcp: frozen_mcp.into_iter().collect(),
        mcp_tools: role
            .mcp
            .iter()
            .filter_map(|server| {
                server
                    .allowed_tools
                    .clone()
                    .map(|tools| (server.id.clone(), tools))
            })
            .collect(),
        required_constraints: role.required_constraints.clone(),
    };
    if policy::evaluate(authority.provider.as_str(), &runtime, &frozen_profile)
        .admit()
        .is_err()
    {
        return refuse("harness policy");
    }
    Ok(())
}

/// Verifies a system parent's assets and native history as its account-owned
/// worker, returning only a digest of the unchanged parent and attempt rows.
/// The worker socket and route fingerprint must match the stored route; no
/// caller-supplied path is used to select the files being verified.
pub fn system_resume_preflight(
    home: &std::path::Path,
    parent: &AgentId,
    worker_socket: &std::path::Path,
    route_fingerprint: &str,
) -> std::result::Result<String, crate::system_launch::ResumePreflightFailure> {
    use crate::system_launch::ResumePreflightFailure as Failure;
    let store = Store::open(home).map_err(|_| Failure::ParentUnverifiable)?;
    let before = agent_run_store::provider_admission::resume_preflight_digest(&store.conn, parent)
        .map_err(|_| Failure::ParentUnverifiable)?;
    let row = store.get(parent).map_err(|_| Failure::ParentUnverifiable)?;
    let frozen = ProviderLaunchIdentity::read(&row).map_err(|_| Failure::ParentUnverifiable)?;
    let route = frozen.system_route.as_ref().ok_or(Failure::RouteChanged)?;
    if route.state_home != home
        || route.worker_socket != worker_socket
        || route.fingerprint.as_str() != route_fingerprint
    {
        return Err(Failure::RouteChanged);
    }
    if !row.status.terminal() {
        return Err(Failure::ParentChanged);
    }
    let runtime_home = frozen
        .runtime_home
        .as_deref()
        .ok_or(Failure::AssetsUnverifiable)?;
    let session = row
        .runtime_session_id
        .as_deref()
        .ok_or(Failure::HistoryUnverifiable)?;
    crate::runtime_storage::verify(
        &store,
        &route.private_home,
        runtime_home,
        frozen.authority.assets_sha256.as_str(),
    )
    .map_err(|_| Failure::AssetsUnverifiable)?;
    let state = store
        .latest_attempt_state(parent)
        .map_err(|_| Failure::HistoryUnverifiable)?;
    let expected_root = (frozen.authority.harness == agent_run_domain::catalog::HarnessId::Codex)
        .then_some(runtime_home);
    verify_recorded_history(&state, &frozen, expected_root, session)
        .map_err(|_| Failure::HistoryUnverifiable)?;
    if agent_run_store::provider_admission::resume_preflight_digest(&store.conn, parent)
        .map_err(|_| Failure::ParentUnverifiable)?
        != before
    {
        return Err(Failure::ParentChanged);
    }
    Ok(before)
}

/// Verifies the native-history seal the supervisor recorded in the parent's
/// last attempt (`adapter_state_json.native_history`) against the file it
/// names. The returned seal must be bound to the parent's session, harness and
/// assets digest, name `expected_root` (when given), and still match the file's
/// exact bytes. A missing seal fails closed; history is never adopted here.
pub(crate) fn verify_recorded_history(
    adapter_state: &str,
    frozen: &ProviderLaunchIdentity,
    expected_root: Option<&std::path::Path>,
    session: &str,
) -> Result<crate::continuity::HistorySeal> {
    let unavailable =
        |reason: &str| Error::Unsupported(format!("continuation_unavailable: {reason}"));
    let state: Value = serde_json::from_str(adapter_state)
        .map_err(|_| unavailable("parent attempt state is unreadable"))?;
    let record = &state["native_history"];
    if record.is_null() {
        return Err(unavailable(
            "the parent attempt recorded no native history seal",
        ));
    }
    let seal: crate::continuity::HistorySeal = serde_json::from_value(record["seal"].clone())
        .map_err(|_| unavailable("the parent's native history seal is malformed"))?;
    if record["assets_sha256"] != frozen.authority.assets_sha256.as_str()
        || record["provider"] != frozen.authority.provider.as_str()
    {
        return Err(unavailable(
            "native history seal is bound to another authority",
        ));
    }
    if expected_root
        .is_some_and(|root| root.canonicalize().ok().as_deref() != Some(seal.root.as_path()))
    {
        return Err(unavailable(
            "native history seal names another storage root",
        ));
    }
    crate::continuity::verify(&seal, frozen.authority.harness, session)?;
    Ok(seal)
}

/// Everything the store needs to admit one provider request, derived without
/// touching the database.
pub(crate) struct Prepared {
    /// The validated request exactly as frozen into the identity.
    pub(crate) request: ProviderStartRequest,
    /// Effective storage request.
    pub(crate) effective: StartRequest,
    /// Resolved launch authority.
    pub(crate) authority: ResolvedLaunchAuthority,
    /// Serialized frozen launch identity.
    pub(crate) identity: Value,
    /// Per-harness active-run cap.
    pub(crate) cap: Option<usize>,
    /// Account pinned by the request label.
    pub(crate) pinned: Option<agent_run_domain::AccountId>,
}

/// Resolves role, authority, effective request and frozen identity for one
/// provider request from an already loaded configuration and catalog, with no
/// database access. Shared by single starts and pool admission so both freeze
/// exactly the same facts. Static executable, credential-reference, role-policy and Codex permission
/// checks reuse the launch implementations before durable admission; no harness
/// or credential is opened. Mutable external state is still verified at spawn.
/// Requested MCP assertions check only the already-resolved role/global cap
/// intersection and never add servers, tools or grants.
/// The effective storage request captures the Codex
/// harness Fast flag without changing original request/replay hashes.
pub(crate) fn prepare_provider(
    config: &ProviderConfig,
    revision: &str,
    catalog: &agent_run_domain::catalog::ProviderCatalog,
    request: ProviderStartRequest,
) -> Result<Prepared> {
    let provider = catalog
        .provider(&request.provider)
        .ok_or_else(|| invalid("provider is not configured"))?;
    let offering = provider
        .models
        .iter()
        .find(|model| model.id == request.model)
        .ok_or_else(|| invalid("model is not offered by provider"))?;
    let effective_effort = request
        .effort
        .clone()
        .or_else(|| offering.params.get("effort").cloned());
    offering.permits_effort(effective_effort.as_deref())?;
    if provider.harness == agent_run_domain::HarnessId::OpenCode {
        adapters::opencode::model_ref(offering.native_model.as_deref().unwrap_or(&offering.id))?;
        if !request.explicit_finish {
            return Err(invalid("OpenCode requires explicit_finish"));
        }
        if effective_effort.is_some() {
            return Err(invalid("OpenCode effort variants are not qualified"));
        }
    }

    // Harness options run through the existing launch mechanisms: fast
    // is the codex service tier, output_schema the claude answer schema.
    if request.fast && provider.harness != agent_run_domain::catalog::HarnessId::Codex {
        return Err(invalid("fast mode is supported only by the codex harness"));
    }
    if request.output_schema.is_some()
        && provider.harness != agent_run_domain::catalog::HarnessId::ClaudeCode
    {
        return Err(invalid(
            "output_schema is supported only by the claude-code harness",
        ));
    }
    let pinned = request
        .account
        .as_ref()
        .map(|label| {
            provider
                .binding(label.as_str())
                .ok_or_else(|| invalid("account label is not bound to provider"))
                .map(|binding| binding.account.clone())
        })
        .transpose()?;
    let mut profile = profiles::load_provider(config, &request)?;
    if request.explicit_finish {
        if let Some(schema) = &request.output_schema {
            agent_run_domain::worker::finish_schema(schema)?;
        }
        profile.body.push_str(include_str!(
            "../../../assets/explicit_finish_instructions.md"
        ));
    }
    profile
        .required_constraints
        .extend(offering.restrictions.iter().copied());
    let skill_sources =
        adapters::provider::effective_skill_sources(config, provider.harness, &profile)?;
    let role = role_plan::resolve_role_plan_with_sources(
        &profile,
        config.skills_dir(),
        &skill_sources,
        &config.mcp,
        if request.account.is_some() {
            "account"
        } else {
            "global"
        },
        request.account.as_ref().map(|label| label.as_str()),
    )?;
    for (server, tools) in &request.required_mcp {
        let selected = role
            .mcp
            .iter()
            .find(|selected| &selected.id == server)
            .ok_or_else(|| invalid("required MCP server is not selected by the resolved role"))?;
        if selected
            .allowed_tools
            .as_ref()
            .is_some_and(|allowed| tools.iter().any(|tool| !allowed.contains(tool)))
        {
            return Err(invalid(
                "required MCP tool is not permitted by the resolved role",
            ));
        }
    }
    let mut effective = request.storage_projection();
    effective.fast |= provider.harness == agent_run_domain::HarnessId::Codex
        && config
            .harnesses
            .get(&provider.harness)
            .is_some_and(|harness| harness.fast_mode);
    effective.effort = effective_effort.clone();
    effective.write = profile.write;
    effective.read_roots = profile.read_roots.clone();
    effective.required_constraints = profile.required_constraints.clone();
    // Compile the same frozen role and native grant used by materialization
    // and the supervisor before any durable row or account reservation exists.
    let runtime = adapters::provider::runtime(config, provider.harness, &request.model)?;
    let launch_profile = adapters::provider::profile(&role);
    adapters::validate_executable(&runtime)?;
    adapters::validate_role(&runtime, &launch_profile)?;
    policy::evaluate(request.provider.as_str(), &runtime, &launch_profile).admit()?;
    if provider.harness == agent_run_domain::HarnessId::Codex {
        crate::codex::Grant::new(&runtime, &effective, &launch_profile, &runtime.home)?;
    }
    let eligible_accounts = provider
        .bindings
        .iter()
        .filter(|binding| {
            binding
                .models
                .as_ref()
                .is_none_or(|models| models.contains(&request.model))
        })
        .filter(|binding| {
            catalog
                .account(&binding.account)
                .is_some_and(|record| record.status == AccountStatus::Enabled)
        })
        .map(|binding| binding.account.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    for account in &eligible_accounts {
        if pinned.as_ref().is_none_or(|pinned| pinned == account) {
            adapters::provider::credential_reference(
                catalog,
                &request.provider,
                &request.model,
                account,
            )?;
        }
    }
    let authority = ResolvedLaunchAuthority {
        provider: request.provider.clone(),
        harness: provider.harness,
        connection: provider.connection.clone(),
        model: request.model.clone(),
        effort: effective_effort,
        profile: profile.name,
        workdir: request.workdir.clone(),
        role_payload: role.to_payload(),
        assets_sha256: Sha256Digest::from_str(&"0".repeat(64))?,
        eligible_accounts,
    };
    let identity = ProviderLaunchIdentity {
        provider_identity_version: 2,
        replay_request_sha256: agent_run_domain::canonical::sha256_hex(
            &serde_json::to_value(&request)?,
            true,
        ),
        provider_request: request.clone(),
        provider_config_sha256: revision.to_owned(),
        provider_config: config.clone(),
        provider_config_snapshot: config.snapshot()?,
        authority: authority.clone(),
        system_route: None,
        runtime_home: None,
        snapshot_sha256: None,
    };
    let cap = config
        .harnesses
        .get(&provider.harness)
        .ok_or_else(|| invalid("provider harness is not configured"))?
        .max_active_agents;
    Ok(Prepared {
        request,
        effective,
        authority,
        identity: serde_json::to_value(identity)?,
        cap,
        pinned,
    })
}

#[cfg(test)]
mod batch_tests {
    use super::*;
    use agent_run_domain::storage::{NotEnqueuedReason, SubmitResult, UnknownCause};

    /// A possible commit remains unknown; a proven unqueued operation does not.
    #[test]
    fn batch_writer_unknown_is_not_a_refusal() {
        let unknown =
            crate::persist::into_result::<()>(SubmitResult::Unknown(UnknownCause::CommitUncertain))
                .unwrap_err();
        let unqueued = crate::persist::into_result::<()>(SubmitResult::NotEnqueued(
            NotEnqueuedReason::DeadlineExpired,
        ))
        .unwrap_err();
        assert!(batch_admission_unknown(&unknown));
        assert!(!batch_admission_unknown(&unqueued));
        let public = Error::BatchOutcomeUnknown {
            source: Box::new(unknown),
        }
        .public();
        assert_eq!(public.kind, "BatchOutcomeUnknown");
        assert!(public.message.contains("outcome unknown"));
    }
}
