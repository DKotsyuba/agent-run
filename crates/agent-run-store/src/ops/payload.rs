//! Payloads and typed results of the closed storage operation inventory.
//!
//! Every struct here is one row of the sole-writer inventory (row numbers are
//! the stable wire ids of [`super::OpKindRef`]). Fields carry only prepared,
//! bounded facts: configuration loading, filesystem, process inspection,
//! hashing and full scans happen in the producer before submission. Remote
//! payloads (decodable from the writer socket) derive `Deserialize`; local
//! payloads are constructed in-process by the broker and only serialize for
//! their digest. Time values are Unix seconds unless named `_ns`.

use agent_run_config::config::Config;
use agent_run_domain::{
    catalog::{
        AccountId, AccountRecord, ProviderCatalog, QuotaCandidateSet, ResolvedLaunchAuthority,
    },
    domain::{AgentId, OrchestratorRef, Outcome, StartRequest},
    pool::{AcceptanceCriterion, PoolDenial, PoolId, PoolMessage, PoolPropose, PoolVote},
    provider_start::ProviderStartRequest,
    quota_snapshot::NormalizedQuotaSnapshot,
    storage::{OperationId, PayloadDigest},
    worker::{FinishGuard, FinishRequest, NotifyRequest, WorkerCatalogProof},
};
use agent_run_platform::{
    process::{Cleanup, Identity, OwnershipSnapshot},
    verify::Proof,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Owned provider admission inputs (the owned form of `AdmissionInputs`).
#[derive(Debug, Clone, Serialize)]
pub struct AdmissionSpec {
    /// Strict provider request exactly as frozen into the identity.
    pub request: ProviderStartRequest,
    /// Effective storage request derived from it.
    pub effective: StartRequest,
    /// Resolved launch authority.
    pub authority: ResolvedLaunchAuthority,
    /// Trusted ranked candidates for the committed capacity revision.
    pub candidates: QuotaCandidateSet,
    /// Frozen non-secret launch identity.
    pub identity: Value,
    /// Global active-run cap.
    pub global_cap: usize,
    /// Optional per-harness active-run cap.
    pub harness_cap: Option<usize>,
    /// Account pinned by the request label, if any.
    pub pinned: Option<AccountId>,
}

impl From<crate::provider_admission::AdmissionInputs<'_>> for AdmissionSpec {
    /// Own the already-prepared inputs before enqueue, so no reader or ranking
    /// borrow survives an asynchronous SQL wait. Values are cloned unchanged;
    /// authorization, capacity and replay predicates remain writer-rechecked.
    fn from(inputs: crate::provider_admission::AdmissionInputs<'_>) -> Self {
        Self {
            request: inputs.request.clone(),
            effective: inputs.effective.clone(),
            authority: inputs.authority.clone(),
            candidates: inputs.candidates.clone(),
            identity: inputs.identity.clone(),
            global_cap: inputs.global_cap,
            harness_cap: inputs.harness_cap,
            pinned: inputs.pinned.cloned(),
        }
    }
}

/// 1 — admit one legacy start request (broker; replay by `request_id`).
#[derive(Debug, Clone, Serialize)]
pub struct AdmitStart {
    /// Validated start request.
    pub request: StartRequest,
    /// Frozen configuration snapshot used for capacity checks.
    pub config: Config,
    /// Frozen configuration revision.
    pub config_revision: String,
    /// Frozen non-secret identity JSON.
    pub identity: Value,
    /// Resume parent execution, if any.
    pub parent: Option<AgentId>,
}

/// Result of [`AdmitStart`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmitStartOutput {
    /// Admitted (or replayed) execution.
    pub agent_id: AgentId,
    /// True when the request id replayed an existing admission.
    pub replayed: bool,
}

/// 2 — admit one provider start (broker; replay by `request_id`).
#[derive(Debug, Clone, Serialize)]
pub struct AdmitProvider {
    /// Resolved catalog.
    pub catalog: ProviderCatalog,
    /// Admission inputs.
    pub inputs: AdmissionSpec,
}

/// 3 — admit one explicit provider resume of a terminal parent.
#[derive(Debug, Clone, Serialize)]
pub struct AdmitProviderResume {
    /// Resolved catalog.
    pub catalog: ProviderCatalog,
    /// Admission inputs.
    pub inputs: AdmissionSpec,
    /// Terminal parent whose native session continues.
    pub parent: AgentId,
    /// Account the parent's last attempt used.
    pub prefer: AccountId,
}

/// Owned pool member admission source.
#[derive(Clone, Serialize)]
pub enum PoolMemberSource {
    /// Ordinary new admission (boxed).
    New(Box<AdmissionSpec>),
    /// Existing running worker pinned by preflight facts (boxed).
    Existing(Box<crate::pool_enrollment::ExistingMember>),
}

/// Owned pool member admission.
#[derive(Clone, Serialize)]
pub struct PoolMemberSpec {
    /// Stable identity reserved before any row exists.
    pub id: AgentId,
    /// One-based slot.
    pub slot: u8,
    /// Unique member name.
    pub name: String,
    /// Role label.
    pub role: String,
    /// The member's own task before the pool preamble.
    pub personal_task: String,
    /// Admission source.
    pub source: PoolMemberSource,
}

/// 4 — admit one whole pool atomically (broker).
#[derive(Clone, Serialize)]
pub struct AdmitPool {
    /// Pre-minted pool identity.
    pub pool_id: PoolId,
    /// Replay namespace.
    pub request_namespace: String,
    /// Client idempotency key.
    pub request_id: String,
    /// Digest of the normalized outer request.
    pub request_sha256: String,
    /// Shared goal.
    pub goal: String,
    /// Acceptance criteria.
    pub acceptance: Vec<AcceptanceCriterion>,
    /// Shared resolved catalog.
    pub catalog: ProviderCatalog,
    /// Members in slot order.
    pub members: Vec<PoolMemberSpec>,
    /// Effective shared binding.
    pub orchestrator: Option<OrchestratorRef>,
}

/// 5 — replace one pool member atomically (broker).
#[derive(Debug, Clone, Serialize)]
pub struct ReplacePoolMember {
    /// Pool identity.
    pub pool_id: PoolId,
    /// Member being replaced.
    pub old: AgentId,
    /// Operator idempotency key.
    pub request_id: String,
    /// Digest of the normalized outer request.
    pub request_sha256: String,
    /// Roster revision the new task was composed against.
    pub expected_roster_revision: u32,
    /// Pre-minted identity of the new member.
    pub new_id: AgentId,
    /// New member name.
    pub name: String,
    /// New member's own task.
    pub personal_task: String,
    /// Resolved catalog.
    pub catalog: ProviderCatalog,
    /// Admission inputs carrying the composed task.
    pub inputs: AdmissionSpec,
}

/// Result of pool replacement or pool writes that may be typed refusals.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", content = "value", rename_all = "snake_case")]
pub enum PoolOutcome<T> {
    /// Durable success.
    Done(T),
    /// Typed pool refusal (no mutation).
    Denied(PoolDenial),
}

/// 6 — enrollment acknowledgement; an internal unit of pool writes, refused standalone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcknowledgeEnrollment {
    /// Lineage root.
    pub root: AgentId,
    /// Acknowledged log sequence.
    pub seq: u64,
}

/// 7 — reconcile enrollments of one pool (broker background).
#[derive(Debug, Clone, Serialize)]
pub struct ReconcilePoolEnrollments {
    /// Pool identity.
    pub pool: PoolId,
}

/// 8 — record the supervisor launch capability hash before spawn (broker).
#[derive(Debug, Clone, Serialize)]
pub struct RecordSupervisorLaunch {
    /// Execution.
    pub execution: AgentId,
    /// SHA-256 of the fresh capability passed over the private pipe.
    pub capability_sha256: String,
}

/// 9 — record the spawned startup owner process identity (broker, before READY).
#[derive(Debug, Clone, Serialize)]
pub struct RecordStartupOwner {
    /// Execution.
    pub execution: AgentId,
    /// Inspected process identity evidence (pid, birth, token, spawn backend).
    pub identity: Value,
}

/// 10 — renew a startup handoff to a new launch (broker).
#[derive(Debug, Clone, Serialize)]
pub struct BeginSupervisorHandoff {
    /// Execution.
    pub execution: AgentId,
    /// New owner identity token.
    pub owner_identity: String,
    /// Observation time.
    pub at: f64,
    /// Handoff deadline in seconds.
    pub deadline_seconds: f64,
    /// SHA-256 of the new launch capability.
    pub capability_sha256: String,
}

/// 11 — claim startup as the recorded startup owner (supervisor S-start).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimStartup {
    /// Execution.
    pub execution: AgentId,
    /// Owner identity token.
    pub owner_identity: String,
    /// Owner birth time.
    pub owner_birth_time: Option<f64>,
    /// Observation time.
    pub at: f64,
    /// Startup deadline in seconds.
    pub deadline_seconds: f64,
}

/// 12 — record the supervisor as owner (supervisor S-start).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordSupervisorOwner {
    /// Execution.
    pub execution: AgentId,
    /// Supervisor pid.
    pub pid: i32,
    /// Supervisor identity token.
    pub identity: String,
    /// Process group id, when the full record form is used.
    pub process_group_id: Option<i32>,
    /// Supervisor birth time.
    pub birth_time: Option<f64>,
    /// Heartbeat time.
    pub at: f64,
}

/// 13 — set the owner of a starting execution (broker).
#[derive(Debug, Clone, Serialize)]
pub struct SetOwner {
    /// Execution.
    pub execution: AgentId,
    /// Owner pid.
    pub pid: i32,
    /// Owner identity token.
    pub identity: String,
    /// Owner birth time.
    pub birth: Option<f64>,
}

/// 14 — sealing-only identity patch with revision compare-and-swap (supervisor).
/// The frozen configuration bytes stay unchanged; only these fields move.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateIdentity {
    /// Execution.
    pub execution: AgentId,
    /// Config revision expected to be current.
    pub expected_revision: String,
    /// New config revision.
    pub new_revision: String,
    /// Materialized runtime home.
    pub runtime_home: String,
    /// Snapshot digest.
    pub snapshot_sha256: String,
    /// Optional new provider authority assets digest.
    pub assets_sha256: Option<String>,
}

/// 15 — starting -> running (supervisor).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Running {
    /// Execution.
    pub execution: AgentId,
    /// Process group id.
    pub pgid: i32,
}

/// 16 — record the native session id (supervisor).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSession {
    /// Execution.
    pub execution: AgentId,
    /// Attempt the event belongs to, when bound.
    pub attempt: Option<String>,
    /// Native session id.
    pub session: String,
}

/// 17 — add captured process members to an owner (supervisor/service/broker).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RememberProcesses {
    /// Owner kind: `attempt`, `service` or `probe`.
    pub kind: String,
    /// Owner id.
    pub owner: String,
    /// Snapshot (<= 2 MiB compact JSON).
    pub snapshot: OwnershipSnapshot,
}

/// 18 — legacy attempt ownership anchor (supervisor; replaces supervisor.rs raw update).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetainCleanupEvidence {
    /// Execution.
    pub execution: AgentId,
    /// Legacy attempt id.
    pub attempt: String,
    /// Leader identity token.
    pub leader_token: String,
    /// Leader birth time.
    pub leader_birth: f64,
}

/// Exact provider attempt target of rows 19-24.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttemptTarget {
    /// Execution.
    pub execution: AgentId,
    /// Attempt.
    pub attempt: String,
}

/// 19 — provider attempt enters spawning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderSpawning {
    /// Target attempt.
    pub target: AttemptTarget,
}

/// 20 — provider attempt never spawned.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderSpawnFailed {
    /// Target attempt.
    pub target: AttemptTarget,
}

/// 21 — provider attempt process leader recorded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderProcess {
    /// Target attempt.
    pub target: AttemptTarget,
    /// Leader identity.
    pub leader: Identity,
}

/// 22 — provider attempt cleanup proof recorded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCleanup {
    /// Target attempt.
    pub target: AttemptTarget,
    /// Verified cleanup proof.
    pub proof: Cleanup,
}

/// Typed continuation metadata of a cleaned-up attempt (no config, no raw log).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProviderHistoryMetadata {
    /// Sealed native history evidence (seal, provider, assets digest, task proof).
    pub native_history: Option<Value>,
    /// Bounded reason when history is unavailable or unprovable.
    pub native_history_unavailable: Option<String>,
    /// Closed native failure facts.
    pub native_failure: Option<Value>,
}

/// 23 — record continuation metadata after cleanup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderHistory {
    /// Target attempt.
    pub target: AttemptTarget,
    /// Metadata.
    pub metadata: ProviderHistoryMetadata,
}

/// 24 — provider attempt running with its process group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderRerunning {
    /// Target attempt.
    pub target: AttemptTarget,
    /// Process group id.
    pub pgid: i32,
}

/// 25 — failover to the next attempt (supervisor).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocateNextAttempt {
    /// Execution.
    pub execution: AgentId,
    /// Frozen catalog as JSON (validated by the store before use).
    pub catalog: Value,
    /// Trusted ranked candidates.
    pub candidates: QuotaCandidateSet,
}

/// Result of [`AllocateNextAttempt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NextAttemptOutput {
    /// New owned attempt id.
    pub attempt_id: String,
    /// One-based number.
    pub number: u32,
    /// Selected account.
    pub account_id: AccountId,
    /// Released previous attempt.
    pub released: String,
}

/// 26 — create the next numbered adapter attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAttempt {
    /// Execution.
    pub execution: AgentId,
    /// Initial state label.
    pub state: String,
    /// Adapter state JSON.
    pub adapter_state: Value,
}

/// 27 — finish one open attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinishAttempt {
    /// Execution.
    pub execution: AgentId,
    /// Attempt.
    pub attempt: String,
    /// Final state label.
    pub state: String,
}

/// 28 — bind a worker capability hash to an owned attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssueWorkerCapability {
    /// Execution.
    pub execution: AgentId,
    /// Attempt.
    pub attempt: String,
    /// Worker bearer token (hashed by the store, never persisted).
    pub token: String,
    /// Issue time.
    pub at: f64,
}

/// 29 — enable explicit-finish lifecycle for an owned attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeginWorkerLifecycle {
    /// Execution.
    pub execution: AgentId,
    /// Attempt.
    pub attempt: String,
    /// Transition time.
    pub at: f64,
}

/// 30 — record the resume usage baseline once.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordResumeUsageBaseline {
    /// Execution.
    pub execution: AgentId,
}

/// 31 — latch an observed native quota exhaustion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatchNativeExhaustion {
    /// Execution whose attempt observed it.
    pub execution: AgentId,
    /// Account.
    pub account: AccountId,
    /// Runtime/provider.
    pub runtime: String,
    /// Lane.
    pub lane: String,
    /// Window.
    pub window: String,
    /// Observation source label.
    pub source: String,
    /// Governed models.
    pub models: BTreeSet<String>,
    /// Observation time.
    pub observed_at: f64,
    /// Reset time when known.
    pub reset_at: Option<f64>,
}

/// One ordered journal fragment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "fragment", rename_all = "snake_case")]
pub enum JournalFragment {
    /// One event row.
    Event {
        /// Event kind.
        kind: String,
        /// Event data.
        data: Value,
        /// Attempt binding, when bound.
        attempt: Option<String>,
    },
    /// One transcript row with already-spooled inline content (<= 32 KiB).
    Message {
        /// Transcript role.
        role: String,
        /// Inline content (spooled stub when the producer spooled).
        content: String,
        /// Whether `content` is the complete body.
        content_complete: bool,
        /// Tool or author name.
        name: Option<String>,
        /// Relative raw spool reference.
        raw_ref: Option<String>,
        /// Attempt binding, when bound.
        attempt: Option<String>,
        /// Explicit tool error/provenance.
        error: Option<(bool, String)>,
    },
}

/// 32 — append ordered journal fragments (<= 32 fragments, <= 2 MiB).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendJournal {
    /// Execution.
    pub execution: AgentId,
    /// Fragments in order.
    pub fragments: Vec<JournalFragment>,
}

/// 33 — enqueue one control command (cancel is Critical, others Normal).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnqueueCommand {
    /// Execution.
    pub execution: AgentId,
    /// Command kind (`cancel`, `steer`, `pool`).
    pub kind: String,
    /// Payload (steer text <= 256 KiB).
    pub payload: Value,
}

/// 34 — claim the next eligible command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimCommand {
    /// Execution.
    pub execution: AgentId,
    /// Whether native-completion wakes are claimable now.
    pub allow_native_wake: bool,
}

/// Result of [`ClaimCommand`]; read the payload with `StoreReader::command_payload`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimedCommand {
    /// Command id.
    pub command_id: i64,
    /// Command kind.
    pub kind: String,
}

/// 35 — complete a claimed command (known-unsent or control completion).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteCommand {
    /// Execution.
    pub execution: AgentId,
    /// Command id.
    pub command_id: i64,
    /// Result JSON.
    pub result: Value,
}

/// 36 — persist an observed native command result atomically.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservedCommandResult {
    /// Execution.
    pub execution: AgentId,
    /// Claimed command id.
    pub command_id: i64,
    /// Result JSON.
    pub result: Value,
    /// Evidence strength: `stdin_written` or `native_accepted`.
    pub strength: String,
    /// Optional turn phase change (`true` idle, `false` running).
    pub worker_turn: Option<bool>,
    /// Journal fragments committed with the result.
    pub fragments: Vec<JournalFragment>,
}

/// 37 — complete pending commands of a terminal execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteTerminalCommands {
    /// Execution.
    pub execution: AgentId,
    /// Completion time.
    pub at: f64,
}

/// 38 — enqueue a native completion wake (deduplicated by receipt digest).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnqueueNativeWake {
    /// Execution.
    pub execution: AgentId,
    /// 64-hex receipt digest.
    pub digest: String,
    /// Wake text (<= 8 KiB).
    pub text: String,
    /// Observation time.
    pub at: f64,
}

/// 39 — explicit-finish turn boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerTurn {
    /// Execution.
    pub execution: AgentId,
    /// True for idle, false for running.
    pub idle: bool,
    /// Observation time.
    pub at: f64,
}

/// 40 — mark the accepted finish receipt observed by the native adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObserveWorkerFinishReceipt {
    /// Execution.
    pub execution: AgentId,
}

/// 41 — persist private recovery evidence under its own identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistRecoveryObservation {
    /// Execution.
    pub execution: AgentId,
    /// Attempt.
    pub attempt: String,
    /// Original operation identity.
    pub original_operation_id: OperationId,
    /// Original payload digest.
    pub original_digest: PayloadDigest,
    /// Closed observation kind.
    pub kind: String,
    /// Closed result facts.
    pub result: Value,
    /// Bounded evidence (<= 64 KiB).
    pub evidence: Value,
}

/// Result of [`PersistRecoveryObservation`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryRecorded {
    /// Whether a new evidence record was written.
    pub recorded: bool,
    /// Original outcome: true committed, false durable rejected, null unknown.
    pub original_committed: Option<bool>,
}

/// 42 — commit one terminal outcome atomically (no stats, no incident capture).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinishTerminal {
    /// Execution.
    pub execution: AgentId,
    /// Terminal outcome.
    pub outcome: Outcome,
    /// Answer proof already verified by the producer.
    pub proof: Option<Proof>,
    /// Final usage payload.
    pub usage: Option<Value>,
}

/// Normalized statistics row computed off the writer thread.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunStatsRow {
    /// Execution.
    pub agent_id: AgentId,
    /// Runtime.
    pub runtime: String,
    /// Model.
    pub model: String,
    /// Profile.
    pub profile: String,
    /// Status.
    pub status: String,
    /// Failure kind.
    pub failure_kind: Option<String>,
    /// Start time.
    pub started_at: Option<f64>,
    /// Finish time.
    pub finished_at: Option<f64>,
    /// Duration in seconds.
    pub duration_seconds: Option<f64>,
    /// Input tokens.
    pub input_tokens: Option<f64>,
    /// Output tokens.
    pub output_tokens: Option<f64>,
    /// Cache read tokens.
    pub cache_read_tokens: Option<f64>,
    /// Cache write tokens.
    pub cache_write_tokens: Option<f64>,
    /// Reasoning tokens.
    pub reasoning_tokens: Option<f64>,
    /// Total tokens.
    pub total_tokens: Option<f64>,
    /// Turns.
    pub num_turns: Option<f64>,
    /// Time to first token in ms.
    pub ttft_ms: Option<f64>,
    /// API duration in ms.
    pub api_duration_ms: Option<f64>,
    /// Cost in USD.
    pub cost_usd: Option<f64>,
    /// Usage source label.
    pub usage_source: String,
    /// Projection time.
    pub recorded_at: f64,
}

/// 43 — persist a reader-computed statistics projection for a terminal execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistRunStats {
    /// Row.
    pub row: RunStatsRow,
}

/// 44 — register worker catalog proof (worker authority; preflighted).
#[derive(Clone, Serialize)]
pub struct RegisterWorkerCatalog {
    /// Proof including the worker token.
    pub proof: WorkerCatalogProof,
    /// Observation time.
    pub at: f64,
}

/// 45 — accept one explicit finish callback (worker authority).
#[derive(Debug, Clone, Serialize)]
pub struct AcceptWorkerFinish {
    /// Execution.
    pub execution: AgentId,
    /// Attempt.
    pub attempt: String,
    /// Worker token.
    pub token: String,
    /// Finish payload.
    pub input: FinishRequest,
    /// Acceptance time.
    pub at: f64,
    /// The broker writer handle's pre-SQL launch-secret verdict. Never
    /// serialized: it is not part of the payload digest, envelope bytes or any
    /// receipt. Constructors pass [`FinishGuard::Unavailable`]; only
    /// [`super::LocalEnvelope::set_finish_guard`] changes it before SQL.
    #[serde(skip)]
    pub guard: FinishGuard,
}

/// 46 — queue one worker report (worker authority).
#[derive(Debug, Clone, Serialize)]
pub struct NotifyOrchestrator {
    /// Execution.
    pub execution: AgentId,
    /// Attempt.
    pub attempt: String,
    /// Worker token.
    pub token: String,
    /// Report.
    pub input: NotifyRequest,
    /// Report time.
    pub at: f64,
}

/// Owned pool write.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "write", content = "value", rename_all = "snake_case")]
pub enum PoolWriteSpec {
    /// Chat message.
    Message(PoolMessage),
    /// Proposal.
    Proposal(PoolPropose),
    /// Vote.
    Vote(PoolVote),
}

/// 47 — authenticated pool write (worker authority).
#[derive(Debug, Clone, Serialize)]
pub struct PoolWrite {
    /// Execution.
    pub execution: AgentId,
    /// Attempt.
    pub attempt: String,
    /// Worker token.
    pub token: String,
    /// Write.
    pub write: PoolWriteSpec,
}

/// Result of pool writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolWriteOutput {
    /// Pool.
    pub pool_id: PoolId,
    /// Entry sequence.
    pub seq: u64,
    /// Whether an existing entry replayed.
    pub duplicate: bool,
}

/// 48 — operator post to a pool (broker).
#[derive(Debug, Clone, Serialize)]
pub struct PoolOperatorPost {
    /// Pool.
    pub pool_id: PoolId,
    /// Idempotency key.
    pub request_id: String,
    /// Message.
    pub message: String,
}

/// 49 — pool fan-out; an internal unit of pool writes, refused standalone.
#[derive(Debug, Clone, Serialize)]
pub struct FanoutEntry {
    /// Pool.
    pub pool_id: PoolId,
    /// Entry sequence.
    pub seq: u64,
}

/// Settlement selector of row 50.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "target", rename_all = "snake_case")]
pub enum SettleTarget {
    /// One pool.
    Pool {
        /// Pool.
        pool_id: PoolId,
    },
    /// The pool of one member.
    Of {
        /// Member execution.
        agent_id: AgentId,
    },
    /// A bounded batch of open pools.
    Open {
        /// Maximum pools (<= 64).
        limit: usize,
        /// Rotation seed.
        seed: usize,
    },
}

/// 50 — proof-gated pool settlement (broker).
#[derive(Debug, Clone, Serialize)]
pub struct SettlePool {
    /// Target.
    pub target: SettleTarget,
}

/// 51 — bind a pool to an orchestrator session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindPool {
    /// Pool.
    pub pool_id: PoolId,
    /// Orchestrator reference.
    pub reference: OrchestratorRef,
    /// Binding time.
    pub at: f64,
}

/// Prepared delivery policy (loaded by the producer, never on the writer thread).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryPolicySpec {
    /// Retry base seconds.
    pub retry_base_seconds: f64,
    /// Retry cap seconds.
    pub retry_cap_seconds: f64,
    /// Maximum attempts (0 unlimited).
    pub max_attempts: u32,
    /// Lease seconds.
    pub lease_seconds: f64,
}

/// 52 — claim the next due delivery lease (broker).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimDelivery {
    /// Lease owner token.
    pub owner: String,
    /// Claim time.
    pub at: f64,
    /// Prepared policy.
    pub policy: DeliveryPolicySpec,
}

/// Dispatch completion facts of row 53.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryCompletion {
    /// Delivery id.
    pub delivery_id: String,
    /// Lease owner.
    pub owner: String,
    /// Attempt number.
    pub attempt: u32,
    /// Canonical transport.
    pub transport: String,
    /// Whether delivery was accepted.
    pub accepted: bool,
    /// Whether the outcome is ambiguous.
    pub ambiguous: bool,
    /// Closed classifier.
    pub classifier: String,
    /// Persisted bounded evidence.
    pub evidence: Option<Value>,
    /// Validated Bridge custody receipt, separate from native delivery evidence.
    #[serde(default)]
    pub bridge_receipt: Option<Value>,
    /// Prepared policy.
    pub policy: DeliveryPolicySpec,
    /// Completion time.
    pub at: f64,
}

/// 53 — record a delivery outcome under its lease (broker).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteDelivery {
    /// Completion facts.
    pub completion: DeliveryCompletion,
}

/// 92 — freeze Bridge ownership and exact publication before any socket write.
/// The claim already froze Bridge ownership; only its live lease can freeze content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareBridgeDelivery {
    /// Existing outbox notification identity.
    pub delivery_id: String,
    /// Live dispatcher lease owner; other owners cannot transfer this intent.
    pub owner: String,
    /// Current owned attempt; crashes before intent preparation never select a new owner.
    pub attempt: u32,
    /// Immutable destination/content/IDs without any credential or caller authority.
    pub intent: agent_run_domain::bridge::BridgeIntent,
    /// Wall-clock time used only to verify that the lease remains live.
    pub at: f64,
}

/// 56 — cancel one delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelDelivery {
    /// Delivery id.
    pub delivery_id: String,
}

/// 57 — expire unbound deliveries (bounded batch).
#[derive(Debug, Clone, Serialize)]
pub struct ExpireUnboundDeliveries {
    /// Time.
    pub at: f64,
}

/// 59 — advance a reconciliation cursor (broker).
#[derive(Debug, Clone, Serialize)]
pub struct ReconcileReapedAgent {
    /// Cursor name.
    pub cursor: String,
    /// Last visited creation time.
    pub created_at: f64,
    /// Last visited execution.
    pub execution: AgentId,
    /// Optional orphan attempt key; must belong to execution before cursor advance.
    pub attempt: Option<String>,
}

/// 60 — release or record unresolved cleanup of an orphaned attempt (broker).
#[derive(Debug, Clone, Serialize)]
pub struct ReleaseOrphanedAttempt {
    /// Execution.
    pub execution: AgentId,
    /// Attempt.
    pub attempt: String,
    /// Proof outcome: `Ok(Some(proof))`, `Ok(None)` recorded proof, or closed reason.
    pub outcome: OrphanOutcome,
}

/// Orphan reconciliation outcome prepared by process observation.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum OrphanOutcome {
    /// Release ownership, optionally storing a new cleanup proof.
    Release {
        /// New cleanup proof.
        proof: Option<Value>,
    },
    /// Record an unresolved cleanup reason once.
    Unresolved {
        /// Closed reason.
        reason: String,
    },
}

/// Expected ownership facts of a lost transition.
#[derive(Debug, Clone, Serialize)]
pub struct ExpectedOwnership {
    /// Execution.
    pub execution: AgentId,
    /// Supervisor pid.
    pub supervisor_pid: Option<i32>,
    /// Supervisor identity.
    pub supervisor_identity: Option<String>,
    /// Supervisor birth time.
    pub supervisor_birth_time: Option<f64>,
    /// Process group.
    pub process_group_id: Option<i32>,
    /// Startup owner identity evidence.
    pub startup_owner: Option<String>,
    /// Startup owner birth time.
    pub startup_birth_time: Option<f64>,
}

/// 61 — proof-gated lost transition (broker).
#[derive(Debug, Clone, Serialize)]
pub struct MarkLost {
    /// Expected ownership.
    pub expected: ExpectedOwnership,
    /// Failure kind.
    pub failure_kind: String,
    /// Failure text.
    pub failure_text: String,
    /// Evidence.
    pub evidence: Value,
    /// Observation time.
    pub checked_at: f64,
}

/// 62 — reconcile an acknowledged finish after supervisor death (broker).
#[derive(Debug, Clone, Serialize)]
pub struct RecoverFinish {
    /// Expected ownership.
    pub expected: ExpectedOwnership,
    /// Verified cleanup proof prepared by the producer.
    pub cleanup: Value,
    /// Verified answer proof prepared by the producer.
    pub proof: Option<Proof>,
    /// Observation time.
    pub checked_at: f64,
}

/// 63 — release leases of finished executions and mark idle generations.
#[derive(Debug, Clone, Serialize)]
pub struct ReleaseFinishedServiceLeases {
    /// Time.
    pub at: f64,
}

/// Step of row 64.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum GateStep {
    /// Acquire a lease on one generation and clear its idle mark.
    Lease {
        /// Generation.
        generation: String,
    },
    /// Publish readiness when all expected leases are held and ready.
    Ready {
        /// Expected lease count.
        expected_leases: u32,
    },
}

/// 64 — prepare one pending service gate.
#[derive(Debug, Clone, Serialize)]
pub struct PrepareServiceGate {
    /// Execution.
    pub execution: AgentId,
    /// Step.
    pub step: GateStep,
    /// Time.
    pub at: f64,
}

/// Action of row 65.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum GenerationAction {
    /// Retire a changed revision that no lease holds.
    Retire {
        /// Generation.
        generation: String,
    },
    /// Create a new starting generation.
    Create {
        /// New generation id.
        generation: String,
        /// Service name.
        service: String,
        /// Revision digest.
        revision: String,
        /// Definition JSON.
        definition_json: String,
        /// Broker identity JSON.
        broker_identity_json: String,
        /// `managed` or `external`.
        ownership: String,
        /// Executor capability hash.
        exec_capability_sha256: Option<String>,
        /// Creation time.
        at: f64,
    },
}

/// 65 — ensure a service generation.
#[derive(Debug, Clone, Serialize)]
pub struct EnsureServiceGeneration {
    /// Action.
    pub action: GenerationAction,
}

/// 66 — retire a generation whose bootstrap could not spawn.
#[derive(Debug, Clone, Serialize)]
pub struct FailServiceSpawn {
    /// Generation.
    pub generation: String,
}

/// 67 — fail one pending gate.
#[derive(Debug, Clone, Serialize)]
pub struct FailServiceGate {
    /// Execution.
    pub execution: AgentId,
    /// Closed reason.
    pub reason: String,
}

/// 68 — mark a generation unhealthy and fail pending gates.
#[derive(Debug, Clone, Serialize)]
pub struct MarkServiceUnhealthy {
    /// Generation.
    pub generation: String,
    /// Closed reason.
    pub reason: String,
    /// Time.
    pub at: f64,
}

/// Observation of row 69.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "observation", rename_all = "snake_case")]
pub enum GenerationObservation {
    /// Record a cleanup proof.
    Cleanup {
        /// Proof JSON.
        proof: Value,
    },
    /// Adopt an external service.
    AdoptExternal,
    /// Record a health check time.
    Checked {
        /// Time.
        at: f64,
    },
}

/// 69 — record one generation observation.
#[derive(Debug, Clone, Serialize)]
pub struct ObserveServiceGeneration {
    /// Generation.
    pub generation: String,
    /// Observation.
    pub observation: GenerationObservation,
}

/// 70 — mark a generation ready.
#[derive(Debug, Clone, Serialize)]
pub struct MarkServiceReady {
    /// Generation.
    pub generation: String,
    /// Time.
    pub at: f64,
}

/// 71 — external generation becomes managed for bootstrap.
#[derive(Debug, Clone, Serialize)]
pub struct ObserveExternalService {
    /// Generation.
    pub generation: String,
    /// Broker identity JSON.
    pub broker_identity_json: String,
    /// New executor hash bound before an external preflight may start a native process.
    pub exec_capability_sha256: Option<String>,
}

/// 72 — stop a generation with its cleanup proof.
#[derive(Debug, Clone, Serialize)]
pub struct StopServiceGeneration {
    /// Generation.
    pub generation: String,
    /// Phase: mark stopping first, then finalize with proof.
    pub finalize: Option<Value>,
    /// Time.
    pub at: f64,
}

/// Probe action of row 73.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ServiceProbeAction {
    /// Register a probe and its ownership snapshot.
    Record {
        /// Probe id.
        probe: String,
        /// Generation.
        generation: String,
        /// Snapshot.
        snapshot: OwnershipSnapshot,
        /// Time.
        at: f64,
    },
    /// Forget a probe after confirmed cleanup.
    Forget {
        /// Probe id.
        probe: String,
    },
}

/// 73 — record or forget a readiness probe.
#[derive(Debug, Clone, Serialize)]
pub struct RecoverServiceProbes {
    /// Action.
    pub action: ServiceProbeAction,
}

/// 74 — service executor records its ownership snapshot (X-start).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapServiceProcess {
    /// Generation.
    pub generation: String,
    /// Snapshot.
    pub snapshot: OwnershipSnapshot,
}

/// 75 — service executor claims the generation process identity (X-start).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimServiceProcessIdentity {
    /// Generation.
    pub generation: String,
    /// Own process identity.
    pub identity: Identity,
}

/// One capacity sample row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapacitySampleRow {
    /// Runtime.
    pub runtime: String,
    /// Lane.
    pub lane: String,
    /// Window.
    pub window: String,
    /// Target.
    pub target: Option<String>,
    /// Source.
    pub source: String,
    /// Remaining percent.
    pub remaining_percent: Option<f64>,
    /// Reset time.
    pub reset_at: Option<f64>,
    /// Observation time.
    pub observed_at: f64,
    /// Validity end.
    pub valid_until: f64,
}

/// 76 — persist one validated capacity slice atomically.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistCapacitySamples {
    /// Runtime.
    pub runtime: String,
    /// Scope id.
    pub scope_id: String,
    /// Observation time.
    pub observed_at: f64,
    /// Validity end.
    pub valid_until: f64,
    /// Validated topology JSON (<= 64 KiB).
    pub topology_json: String,
    /// Samples (<= 256).
    pub samples: Vec<CapacitySampleRow>,
    /// Retention row bound.
    pub retention: usize,
}

/// 77 — trim capacity history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneCapacitySamples {
    /// Retention row bound.
    pub retention: usize,
}

/// 78 — persist one normalized quota snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordQuotaSnapshot {
    /// Runtime.
    pub runtime: String,
    /// Snapshot.
    pub snapshot: NormalizedQuotaSnapshot,
    /// Retention row bound.
    pub retention: usize,
    /// Evaluation time.
    pub at: f64,
}

/// 79 — exhaustion retention; an internal unit of row 78, refused standalone.
#[derive(Debug, Clone, Serialize)]
pub struct RetainExhausted {
    /// Account.
    pub account: AccountId,
}

/// 80 — register an account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterAccount {
    /// Record.
    pub record: AccountRecord,
}

/// 81 — disable an account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisableAccount {
    /// Account.
    pub account: AccountId,
}

/// 82 — bind an execution to an orchestrator session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindOrchestrator {
    /// Execution.
    pub execution: AgentId,
    /// Reference.
    pub reference: OrchestratorRef,
    /// Time.
    pub at: f64,
}

/// 83 — record context components for a session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordContextComponents {
    /// Reference.
    pub reference: OrchestratorRef,
    /// Components.
    pub components: BTreeMap<String, String>,
    /// Time.
    pub at: f64,
}

/// Session selector of row 84.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "session", rename_all = "snake_case")]
pub enum SessionTarget {
    /// Existing session row id.
    Id {
        /// Session id.
        id: String,
    },
    /// External reference.
    Reference {
        /// Reference.
        reference: OrchestratorRef,
    },
}

/// 84 — record a context receipt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordContextReceipt {
    /// Session.
    pub target: SessionTarget,
    /// Context key.
    pub context_key: String,
    /// Time.
    pub at: f64,
}

/// One bounded incident phase projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncidentPhase {
    /// Phase name.
    pub phase: String,
    /// Occurrence time.
    pub occurred_at: f64,
    /// Closed details (<= 4096 bytes).
    pub details: Value,
}

/// Incident projection of one execution computed off the writer thread.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncidentProjection {
    /// Execution.
    pub execution: AgentId,
    /// Phases (<= 8).
    pub phases: Vec<IncidentPhase>,
}

/// 85 — prune one bounded history batch, persisting carried incident projections.
#[derive(Debug, Clone, Serialize)]
pub struct PruneHistoryBatch {
    /// Time.
    pub at: f64,
    /// Candidate executions prepared by the reader.
    pub candidates: Vec<AgentId>,
    /// Incident projections for the candidates.
    pub projections: Vec<IncidentProjection>,
}

/// 86 — prune one incident ledger batch.
#[derive(Debug, Clone, Serialize)]
pub struct PruneIncidentLedger {
    /// Time.
    pub at: f64,
}

/// 87 — prune stale unscoped receipts and watermarks.
#[derive(Debug, Clone, Serialize)]
pub struct PruneReceipts {
    /// Wall time.
    pub at: f64,
    /// Current boot id.
    pub boot_id: String,
    /// Current monotonic ns.
    pub now_ns: u64,
}

/// 88 — prepare a runtime storage layout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareRuntimeLayout {
    /// Canonical layout JSON.
    pub layout_json: String,
    /// Owner execution.
    pub owner: Option<AgentId>,
}

/// 89 — commit a prepared runtime storage layout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitRuntimeLayout {
    /// Runtime home.
    pub runtime_home: String,
    /// Operation token.
    pub operation_token: String,
    /// Layout digest.
    pub layout_sha256: String,
}

/// Result of rows 88/89.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayoutReceipt {
    /// Runtime home.
    pub runtime_home: String,
    /// Layout digest.
    pub layout_sha256: String,
    /// State label.
    pub state: String,
    /// Operation token.
    pub operation_token: String,
}

/// 90 — remove a runtime storage layout.
#[derive(Debug, Clone, Serialize)]
pub struct RemoveRuntimeLayout {
    /// Runtime home.
    pub runtime_home: String,
}

/// 91 — persist an incident projection best-effort after terminal.
#[derive(Debug, Clone, Serialize)]
pub struct CaptureIncident {
    /// Projection.
    pub projection: IncidentProjection,
}

/// 93 — reconcile one exact current attempt's bounded health checks (broker only).
#[derive(Debug, Clone, Serialize)]
pub struct HealthTick {
    /// Consistent, bounded read facts prepared outside the sole writer.
    pub facts: crate::health::HealthFacts,
    /// Exact execution selected before enqueue; attempt/phase are rechecked in SQL.
    pub execution: AgentId,
    /// Finite nonnegative observation time in Unix seconds.
    pub at: f64,
}

/// 94 — authenticate one model probe reply through the existing worker capability.
#[derive(Debug, Clone, Serialize)]
pub struct AcceptHealthReply {
    /// Exact execution, never an implicitly resolved latest run.
    pub execution: AgentId,
    /// Exact active attempt carrying the challenge.
    pub attempt: String,
    /// Ephemeral private capability, never stored verbatim or logged.
    pub token: String,
    /// Closed bounded response; no arbitrary prose or credentials.
    pub input: agent_run_domain::health::PingReply,
    /// Finite nonnegative observation time in Unix seconds.
    pub at: f64,
}
