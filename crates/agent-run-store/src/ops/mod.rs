//! Closed storage operation inventory of the single resident writer.
//!
//! Every online mutation is one typed operation with a stable wire id
//! ([`crate::ops::OpKindRef`]). Operations a peer process may submit over the writer
//! socket form [`crate::ops::RemoteOp`], the only wire-decodable set; broker-only
//! operations form [`crate::ops::LocalOp`] and cannot be expressed in a frame. There is no
//! SQL text, table name, closure or generic execute/transaction operation.
//!
//! Envelopes ([`crate::ops::WriteEnvelope`], [`crate::ops::LocalEnvelope`]) are constructed only by
//! their constructors, which compute the canonical digest and encoded size in
//! the producer or transport thread; the writer thread never hashes, encodes
//! requests or reads files. A queued remote envelope retains only its
//! verified canonical body bytes.

pub(crate) mod apply;
pub(crate) mod dispatch;
mod payload;

pub use apply::{Degradation, Executed, lookup};
pub use dispatch::implemented;
pub use payload::*;

use agent_run_domain::{
    Result,
    canonical::dumps,
    error::invalid,
    storage::{
        MonoDeadline, OperationId, PayloadDigest, ProducerId, RejectCode, RejectStage,
        StorageRejection, WRITE_PROTOCOL, WriteAuthority, WriteClass,
    },
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::sync::Arc;

/// Private seal: only this crate implements [`WriteOp`].
mod sealed {
    /// Marker implemented for every inventory payload of this crate.
    pub trait Sealed {}
}

/// One typed operation of the closed inventory.
///
/// Implemented only inside this crate (sealed). `Output` is the typed result
/// stored in the receipt as canonical JSON and returned by
/// `WriteReceipt::output::<Output>()`.
pub trait WriteOp: sealed::Sealed + Serialize {
    /// Typed result stored in the receipt.
    type Output: Serialize + DeserializeOwned;
    /// Stable inventory row.
    const KIND: OpKindRef;
}

/// Declares the inventory table once: wire id, name, wire set, default class
/// and receipt scope (DOC-003).
macro_rules! inventory {
    ($($id:literal $name:ident $snake:literal $set:ident $class:ident $scoped:ident;)*) => {
        /// Stable inventory row; the discriminant is the wire id, never reused.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[repr(u16)]
        pub enum OpKindRef {
            $(
                #[doc = concat!("Inventory row ", stringify!($id), ".")]
                $name = $id,
            )*
        }

        impl OpKindRef {
            /// Stable wire id of this row.
            pub fn wire_id(self) -> u16 {
                self as u16
            }

            /// Row for a wire id; unknown ids are `None`.
            pub fn from_wire_id(id: u16) -> Option<Self> {
                match id {
                    $($id => Some(Self::$name),)*
                    _ => None,
                }
            }

            /// Snake-case operation kind name stored in receipts.
            pub fn name(self) -> &'static str {
                match self {
                    $(Self::$name => $snake,)*
                }
            }

            /// Preliminary default class. Never authoritative for payload
            /// dependent rows (row 33); use the envelope's verified class.
            pub fn class(self) -> WriteClass {
                match self {
                    $(Self::$name => inventory!(@class $class),)*
                }
            }

            /// Whether the row belongs to the wire-decodable [`RemoteOp`] set.
            pub fn is_remote(self) -> bool {
                match self {
                    $(Self::$name => inventory!(@remote $set),)*
                }
            }

            /// Whether receipts of this row are scoped to its exact execution
            /// (DOC-003). Scoped rows reject a deleted execution as
            /// non-durable `UnknownScope` before any mutation.
            pub fn scoped(self) -> bool {
                match self {
                    $(Self::$name => inventory!(@scoped $scoped),)*
                }
            }

            /// Every row in wire id order.
            pub fn all() -> &'static [Self] {
                &[$(Self::$name,)*]
            }
        }
    };
    (@class C) => { WriteClass::Critical };
    (@class N) => { WriteClass::Normal };
    (@class B) => { WriteClass::Background };
    (@remote R) => { true };
    (@remote L) => { false };
    (@scoped y) => { true };
    (@scoped n) => { false };
}

inventory! {
    1 AdmitStart "admit_start" L N n;
    2 AdmitProvider "admit_provider" L N n;
    3 AdmitProviderResume "admit_provider_resume" L N n;
    4 AdmitPool "admit_pool" L N n;
    5 ReplacePoolMember "replace_pool_member" L N n;
    6 AcknowledgeEnrollment "acknowledge_enrollment" L N n;
    7 ReconcilePoolEnrollments "reconcile_pool_enrollments" L B n;
    8 RecordSupervisorLaunch "record_supervisor_launch" L C y;
    9 RecordStartupOwner "record_startup_owner" L C y;
    10 BeginSupervisorHandoff "begin_supervisor_handoff" L C y;
    11 ClaimStartup "claim_startup" R C y;
    12 RecordSupervisorOwner "record_supervisor_owner" R C y;
    13 SetOwner "set_owner" L C y;
    14 UpdateIdentity "update_identity" R C y;
    15 Running "running" R C y;
    16 RuntimeSession "runtime_session" R C y;
    17 RememberProcesses "remember_processes" R C n;
    18 RetainCleanupEvidence "retain_cleanup_evidence" R C y;
    19 ProviderSpawning "provider_spawning" R C y;
    20 ProviderSpawnFailed "provider_spawn_failed" R C y;
    21 ProviderProcess "provider_process" R C y;
    22 ProviderCleanup "provider_cleanup" R C y;
    23 ProviderHistory "provider_history" R C y;
    24 ProviderRerunning "provider_rerunning" R C y;
    25 AllocateNextAttempt "allocate_next_attempt" R C n;
    26 CreateAttempt "create_attempt" R C n;
    27 FinishAttempt "finish_attempt" R C y;
    28 IssueWorkerCapability "issue_worker_capability" R C n;
    29 BeginWorkerLifecycle "begin_worker_lifecycle" R C n;
    30 RecordResumeUsageBaseline "record_resume_usage_baseline" R N n;
    31 LatchNativeExhaustion "latch_native_exhaustion" R C n;
    32 AppendJournal "append_journal" R N n;
    33 EnqueueCommand "enqueue_command" R N n;
    34 ClaimCommand "claim_command" R C y;
    35 CompleteCommand "complete_command" R C y;
    36 ObservedCommandResult "observed_command_result" R C n;
    37 CompleteTerminalCommands "complete_terminal_commands" R C y;
    38 EnqueueNativeWake "enqueue_native_wake" R C n;
    39 WorkerTurn "worker_turn" R N y;
    40 ObserveWorkerFinishReceipt "observe_worker_finish_receipt" R C y;
    41 PersistRecoveryObservation "persist_recovery_observation" R C n;
    42 FinishTerminal "finish_terminal" R C y;
    43 PersistRunStats "persist_run_stats" R B n;
    44 RegisterWorkerCatalog "register_worker_catalog" L N y;
    45 AcceptWorkerFinish "accept_worker_finish" L N y;
    46 NotifyOrchestrator "notify_orchestrator" L N n;
    47 PoolWrite "pool_write" L N n;
    48 PoolOperatorPost "pool_operator_post" L N n;
    49 FanoutEntry "fanout_entry" L N n;
    50 SettlePool "settle_pool" L N n;
    51 BindPool "bind_pool" R N n;
    52 ClaimDelivery "claim_delivery" R N n;
    53 CompleteDelivery "complete_delivery" R N n;
    56 CancelDelivery "cancel_delivery" R N n;
    57 ExpireUnboundDeliveries "expire_unbound_deliveries" L B n;
    59 ReconcileReapedAgent "reconcile_reaped_agent" L C y;
    60 ReleaseOrphanedAttempt "release_orphaned_attempt" L C y;
    61 MarkLost "mark_lost" L C y;
    62 RecoverFinish "recover_finish" L C y;
    63 ReleaseFinishedServiceLeases "release_finished_service_leases" L B n;
    64 PrepareServiceGate "prepare_service_gate" L N n;
    65 EnsureServiceGeneration "ensure_service_generation" L N n;
    66 FailServiceSpawn "fail_service_spawn" L N n;
    67 FailServiceGate "fail_service_gate" L N n;
    68 MarkServiceUnhealthy "mark_service_unhealthy" L N n;
    69 ObserveServiceGeneration "observe_service_generation" L N n;
    70 MarkServiceReady "mark_service_ready" L C n;
    71 ObserveExternalService "observe_external_service" L N n;
    72 StopServiceGeneration "stop_service_generation" L C n;
    73 RecoverServiceProbes "recover_service_probes" L C n;
    74 BootstrapServiceProcess "bootstrap_service_process" R C n;
    75 ClaimServiceProcessIdentity "claim_service_process_identity" R C n;
    76 PersistCapacitySamples "persist_capacity_samples" R B n;
    77 PruneCapacitySamples "prune_capacity_samples" R B n;
    78 RecordQuotaSnapshot "record_quota_snapshot" R B n;
    79 RetainExhausted "retain_exhausted" L B n;
    80 RegisterAccount "register_account" R N n;
    81 DisableAccount "disable_account" R N n;
    82 BindOrchestrator "bind_orchestrator" R N n;
    83 RecordContextComponents "record_context_components" R N n;
    84 RecordContextReceipt "record_context_receipt" R N n;
    85 PruneHistoryBatch "prune_history_batch" L B n;
    86 PruneIncidentLedger "prune_incident_ledger" L B n;
    87 PruneReceipts "prune_receipts" L B n;
    88 PrepareRuntimeLayout "prepare_runtime_layout" R N n;
    89 CommitRuntimeLayout "commit_runtime_layout" R N n;
    90 RemoveRuntimeLayout "remove_runtime_layout" L B n;
    91 CaptureIncident "capture_incident" L B n;
    92 PrepareBridgeDelivery "prepare_bridge_delivery" R N n;
    93 HealthTick "health_tick" L B y;
    94 AcceptHealthReply "accept_health_reply" L N y;
    95 ReportPoolReadLoop "report_pool_read_loop" L N y;
}

/// Seals one payload and binds it to its inventory row and typed output.
macro_rules! op {
    ($($name:ident => $output:ty;)*) => {
        $(
            impl sealed::Sealed for $name {}
            impl WriteOp for $name {
                type Output = $output;
                const KIND: OpKindRef = OpKindRef::$name;
            }
        )*
    };
}

op! {
    AdmitStart => AdmitStartOutput;
    AdmitProvider => crate::provider_admission::ProviderAdmission;
    AdmitProviderResume => crate::provider_admission::ProviderAdmission;
    AdmitPool => crate::pool_admission::PoolAdmission;
    ReplacePoolMember => PoolOutcome<crate::pool_replace::PoolReplacement>;
    AcknowledgeEnrollment => ();
    ReconcilePoolEnrollments => ();
    RecordSupervisorLaunch => ();
    RecordStartupOwner => ();
    BeginSupervisorHandoff => bool;
    ClaimStartup => ();
    RecordSupervisorOwner => ();
    SetOwner => ();
    UpdateIdentity => ();
    Running => ();
    RuntimeSession => ();
    RememberProcesses => ();
    RetainCleanupEvidence => ();
    ProviderSpawning => bool;
    ProviderSpawnFailed => ();
    ProviderProcess => ();
    ProviderCleanup => ();
    ProviderHistory => ();
    ProviderRerunning => ();
    AllocateNextAttempt => NextAttemptOutput;
    CreateAttempt => String;
    FinishAttempt => ();
    IssueWorkerCapability => ();
    BeginWorkerLifecycle => ();
    RecordResumeUsageBaseline => ();
    LatchNativeExhaustion => i64;
    AppendJournal => Vec<i64>;
    EnqueueCommand => Value;
    ClaimCommand => Option<ClaimedCommand>;
    CompleteCommand => ();
    ObservedCommandResult => Vec<i64>;
    CompleteTerminalCommands => u64;
    EnqueueNativeWake => bool;
    WorkerTurn => bool;
    ObserveWorkerFinishReceipt => ();
    PersistRecoveryObservation => RecoveryRecorded;
    FinishTerminal => String;
    PersistRunStats => ();
    RegisterWorkerCatalog => ();
    AcceptWorkerFinish => agent_run_domain::worker::FinishReceipt;
    HealthTick => bool;
    AcceptHealthReply => bool;
    ReportPoolReadLoop => bool;
    NotifyOrchestrator => agent_run_domain::worker::NotifyReceipt;
    PoolWrite => PoolOutcome<PoolWriteOutput>;
    PoolOperatorPost => PoolOutcome<PoolWriteOutput>;
    FanoutEntry => ();
    SettlePool => Value;
    BindPool => String;
    ClaimDelivery => Option<Value>;
    CompleteDelivery => String;
    PrepareBridgeDelivery => agent_run_domain::bridge::BridgeIntent;
    CancelDelivery => bool;
    ExpireUnboundDeliveries => Vec<String>;
    ReconcileReapedAgent => ();
    ReleaseOrphanedAttempt => bool;
    MarkLost => bool;
    RecoverFinish => bool;
    ReleaseFinishedServiceLeases => ();
    PrepareServiceGate => bool;
    EnsureServiceGeneration => Option<String>;
    FailServiceSpawn => ();
    FailServiceGate => ();
    MarkServiceUnhealthy => ();
    ObserveServiceGeneration => ();
    MarkServiceReady => ();
    ObserveExternalService => ();
    StopServiceGeneration => String;
    RecoverServiceProbes => ();
    BootstrapServiceProcess => ();
    ClaimServiceProcessIdentity => bool;
    PersistCapacitySamples => usize;
    PruneCapacitySamples => usize;
    RecordQuotaSnapshot => i64;
    RetainExhausted => ();
    RegisterAccount => ();
    DisableAccount => ();
    BindOrchestrator => String;
    RecordContextComponents => (String, Vec<String>);
    RecordContextReceipt => (String, bool);
    PruneHistoryBatch => Value;
    PruneIncidentLedger => usize;
    PruneReceipts => usize;
    PrepareRuntimeLayout => LayoutReceipt;
    CommitRuntimeLayout => LayoutReceipt;
    RemoveRuntimeLayout => bool;
    CaptureIncident => bool;
}

/// Declares one tagged operation enum over a list of payload structs.
macro_rules! op_enum {
    ($(#[$meta:meta])* $vis:vis enum $enum:ident { $($name:ident,)* }) => {
        $(#[$meta])*
        #[serde(tag = "kind", content = "op", rename_all = "snake_case")]
        $vis enum $enum {
            $(
                #[doc = concat!("See [`", stringify!($name), "`].")]
                $name($name),
            )*
        }

        impl $enum {
            /// Inventory row of the contained payload.
            pub fn kind(&self) -> OpKindRef {
                match self {
                    $(Self::$name(_) => OpKindRef::$name,)*
                }
            }
        }

        $(
            impl From<$name> for $enum {
                /// Wraps the payload in its closed variant.
                fn from(op: $name) -> Self {
                    Self::$name(op)
                }
            }
        )*
    };
}

op_enum! {
    /// Operations a peer process may submit over the writer socket; the only
    /// wire-decodable set. The broker may also submit them in-process.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum RemoteOp {
        ClaimStartup,
        RecordSupervisorOwner,
        UpdateIdentity,
        Running,
        RuntimeSession,
        RememberProcesses,
        RetainCleanupEvidence,
        ProviderSpawning,
        ProviderSpawnFailed,
        ProviderProcess,
        ProviderCleanup,
        ProviderHistory,
        ProviderRerunning,
        AllocateNextAttempt,
        CreateAttempt,
        FinishAttempt,
        IssueWorkerCapability,
        BeginWorkerLifecycle,
        RecordResumeUsageBaseline,
        LatchNativeExhaustion,
        AppendJournal,
        EnqueueCommand,
        ClaimCommand,
        CompleteCommand,
        ObservedCommandResult,
        CompleteTerminalCommands,
        EnqueueNativeWake,
        WorkerTurn,
        ObserveWorkerFinishReceipt,
        PersistRecoveryObservation,
        FinishTerminal,
        PersistRunStats,
        BindPool,
        ClaimDelivery,
        CompleteDelivery,
        PrepareBridgeDelivery,
        PruneCapacitySamples,
        CancelDelivery,
        BootstrapServiceProcess,
        ClaimServiceProcessIdentity,
        PersistCapacitySamples,
        RecordQuotaSnapshot,
        RegisterAccount,
        DisableAccount,
        BindOrchestrator,
        RecordContextComponents,
        RecordContextReceipt,
        PrepareRuntimeLayout,
        CommitRuntimeLayout,
    }
}

op_enum! {
    /// Broker-only operations, constructed in-process and never decodable
    /// from a request frame. Rows 44-47 and 94-95 require an explicit `Worker`
    /// authority; all others require `Broker`.
    #[derive(Clone, Serialize)]
    pub enum LocalOp {
        AdmitStart,
        AdmitProvider,
        AdmitProviderResume,
        AdmitPool,
        ReplacePoolMember,
        AcknowledgeEnrollment,
        ReconcilePoolEnrollments,
        RecordSupervisorLaunch,
        RecordStartupOwner,
        BeginSupervisorHandoff,
        SetOwner,
        RegisterWorkerCatalog,
        AcceptWorkerFinish,
        HealthTick,
        AcceptHealthReply,
        ReportPoolReadLoop,
        NotifyOrchestrator,
        PoolWrite,
        PoolOperatorPost,
        FanoutEntry,
        SettlePool,
        ExpireUnboundDeliveries,
        ReconcileReapedAgent,
        ReleaseOrphanedAttempt,
        MarkLost,
        RecoverFinish,
        ReleaseFinishedServiceLeases,
        PrepareServiceGate,
        EnsureServiceGeneration,
        FailServiceSpawn,
        FailServiceGate,
        MarkServiceUnhealthy,
        ObserveServiceGeneration,
        MarkServiceReady,
        ObserveExternalService,
        StopServiceGeneration,
        RecoverServiceProbes,
        RetainExhausted,
        PruneHistoryBatch,
        PruneIncidentLedger,
        PruneReceipts,
        RemoveRuntimeLayout,
        CaptureIncident,
    }
}

impl OpKindRef {
    /// Decoded op bound D in compact UTF-8 JSON bytes (contract section 5a).
    pub fn decoded_max(self) -> usize {
        const KIB: usize = 1024;
        const MIB: usize = 1024 * KIB;
        match self {
            Self::RememberProcesses
            | Self::AppendJournal
            | Self::ObservedCommandResult
            | Self::AllocateNextAttempt
            | Self::ProviderHistory => 2 * MIB,
            Self::EnqueueCommand => 260 * KIB,
            Self::FinishTerminal => 512 * KIB,
            Self::RecordQuotaSnapshot
            | Self::PersistCapacitySamples
            | Self::PrepareRuntimeLayout
            | Self::CommitRuntimeLayout => MIB,
            Self::BindOrchestrator
            | Self::RecordContextComponents
            | Self::RecordContextReceipt
            | Self::RegisterAccount => 256 * KIB,
            Self::AdmitStart
            | Self::AdmitProvider
            | Self::AdmitProviderResume
            | Self::AdmitPool
            | Self::ReplacePoolMember => 16 * MIB,
            kind if kind.is_remote() => 64 * KIB,
            _ => 2 * MIB,
        }
    }

    /// Canonical wire body maximum `min(16 MiB, 6 x D + 64 KiB)`; zero for
    /// local rows, which are never accepted from the wire.
    pub fn wire_max(self) -> usize {
        if !self.is_remote() {
            return 0;
        }
        (6 * self.decoded_max() + 64 * 1024).min(16 * 1024 * 1024)
    }

    /// Whether this row is an internal unit of another operation that is
    /// refused when submitted on its own (rows 6, 49 and 79).
    pub fn internal_only(self) -> bool {
        matches!(
            self,
            Self::AcknowledgeEnrollment | Self::FanoutEntry | Self::RetainExhausted
        )
    }

    /// Static authority matrix: whether `authority` may submit this row at
    /// all. Scope predicates (capability, process identity, execution) are
    /// re-checked inside the write transaction.
    pub fn permits(self, authority: &WriteAuthority) -> bool {
        use WriteAuthority as A;
        let broker = matches!(authority, A::Broker);
        let supervisor = matches!(authority, A::Supervisor { .. });
        match self {
            Self::RegisterWorkerCatalog
            | Self::AcceptWorkerFinish
            | Self::AcceptHealthReply
            | Self::ReportPoolReadLoop
            | Self::NotifyOrchestrator
            | Self::PoolWrite => matches!(authority, A::Worker { .. }),
            kind if !kind.is_remote() => broker,
            Self::ClaimStartup
            | Self::RecordSupervisorOwner
            | Self::UpdateIdentity
            | Self::Running
            | Self::RuntimeSession
            | Self::RetainCleanupEvidence
            | Self::ProviderSpawning
            | Self::ProviderSpawnFailed
            | Self::ProviderProcess
            | Self::ProviderCleanup
            | Self::ProviderHistory
            | Self::ProviderRerunning
            | Self::AllocateNextAttempt
            | Self::CreateAttempt
            | Self::FinishAttempt
            | Self::IssueWorkerCapability
            | Self::BeginWorkerLifecycle
            | Self::RecordResumeUsageBaseline
            | Self::LatchNativeExhaustion
            | Self::ClaimCommand
            | Self::CompleteCommand
            | Self::ObservedCommandResult
            | Self::EnqueueNativeWake
            | Self::WorkerTurn
            | Self::ObserveWorkerFinishReceipt => supervisor,
            Self::AppendJournal
            | Self::CompleteTerminalCommands
            | Self::PersistRecoveryObservation
            | Self::FinishTerminal
            | Self::PersistRunStats
            | Self::PrepareRuntimeLayout
            | Self::CommitRuntimeLayout => supervisor || broker,
            Self::RememberProcesses => {
                supervisor || broker || matches!(authority, A::ServiceExec { .. })
            }
            Self::ClaimDelivery
            | Self::CompleteDelivery
            | Self::PrepareBridgeDelivery
            | Self::EnqueueCommand
            | Self::BindPool
            | Self::CancelDelivery
            | Self::BindOrchestrator => broker || matches!(authority, A::Cli { .. }),
            Self::RegisterAccount
            | Self::DisableAccount
            | Self::RecordContextComponents
            | Self::RecordContextReceipt => matches!(authority, A::Cli { .. }),
            Self::BootstrapServiceProcess | Self::ClaimServiceProcessIdentity => {
                matches!(authority, A::ServiceExec { .. })
            }
            Self::PersistCapacitySamples | Self::PruneCapacitySamples => {
                broker || matches!(authority, A::Collector { .. })
            }
            Self::RecordQuotaSnapshot => {
                broker || supervisor || matches!(authority, A::Collector { .. })
            }
            _ => false,
        }
    }
}

impl RemoteOp {
    /// Authoritative class computed from the verified payload: row 33 is
    /// Critical for `cancel` and Normal otherwise; other rows use their
    /// inventory class. Header flags or lanes never change it.
    pub fn class(&self) -> WriteClass {
        match self {
            Self::EnqueueCommand(op) if op.kind == "cancel" => WriteClass::Critical,
            other => other.kind().class(),
        }
    }
}

impl LocalOp {
    /// Authoritative class of a broker-local payload (its inventory class).
    pub fn class(&self) -> WriteClass {
        self.kind().class()
    }

    /// Execution and service generation the payload targets, for the
    /// scheduler's reservation, large-job and barrier scope. Service
    /// management rows target their generation (probes included).
    pub fn target(&self) -> (Option<agent_run_domain::domain::AgentId>, Option<String>) {
        let generation = match self {
            Self::FailServiceSpawn(op) => Some(op.generation.clone()),
            Self::MarkServiceUnhealthy(op) => Some(op.generation.clone()),
            Self::ObserveServiceGeneration(op) => Some(op.generation.clone()),
            Self::MarkServiceReady(op) => Some(op.generation.clone()),
            Self::ObserveExternalService(op) => Some(op.generation.clone()),
            Self::StopServiceGeneration(op) => Some(op.generation.clone()),
            Self::RecoverServiceProbes(op) => match &op.action {
                ServiceProbeAction::Record { generation, .. } => Some(generation.clone()),
                ServiceProbeAction::Forget { .. } => None,
            },
            Self::EnsureServiceGeneration(op) => match &op.action {
                GenerationAction::Retire { generation }
                | GenerationAction::Create { generation, .. } => Some(generation.clone()),
            },
            _ => None,
        };
        let (execution, _, facts_generation) = dispatch::local_facts(self);
        (execution, generation.or(facts_generation))
    }
}

impl RemoteOp {
    /// Execution and service generation the payload targets, for the
    /// scheduler's reservation, large-job and barrier scope. A service
    /// process snapshot targets its generation.
    pub fn target(&self) -> (Option<agent_run_domain::domain::AgentId>, Option<String>) {
        match self {
            Self::RememberProcesses(op) if op.kind == "service" => (None, Some(op.owner.clone())),
            other => {
                let (execution, _, generation) = dispatch::remote_facts(other);
                (execution, generation)
            }
        }
    }
}

/// Header fields shared by both envelope kinds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnvelopeHeader {
    /// Protocol version.
    pub protocol: u16,
    /// Operation identity.
    pub operation_id: OperationId,
    /// Producer.
    pub producer: ProducerId,
    /// Producer ordinal.
    pub ordinal: u64,
    /// Original absolute deadline.
    pub deadline: MonoDeadline,
    /// Digest of the operation.
    pub digest: PayloadDigest,
}

/// Canonical wire body of a remote envelope.
#[derive(Serialize, Deserialize)]
struct WireBody {
    /// Header fields.
    #[serde(flatten)]
    header: EnvelopeHeader,
    /// Operation.
    op: RemoteOp,
}

/// Validates producer, ordinal and the decoded bound, and returns the
/// canonical digest of `op_value`.
fn checked_digest(
    producer: &ProducerId,
    ordinal: u64,
    kind: OpKindRef,
    op_value: &Value,
) -> Result<PayloadDigest> {
    producer.validate()?;
    if ordinal > i64::MAX as u64 {
        return Err(invalid("producer ordinal out of range"));
    }
    if serde_json::to_vec(op_value)?.len() > kind.decoded_max() {
        return Err(invalid("operation payload exceeds its decoded bound"));
    }
    Ok(PayloadDigest::of(op_value))
}

/// A remote operation submission with its verified canonical body.
///
/// Fields are private: the only constructors are [`WriteEnvelope::new`]
/// (producer side) and [`WriteEnvelope::from_wire`] (transport side), both of
/// which compute the digest off the writer thread. Only the canonical body
/// bytes are retained; [`WriteEnvelope::decode`] reproduces the operation.
#[derive(Debug, Clone)]
pub struct WriteEnvelope {
    /// Header fields.
    header: EnvelopeHeader,
    /// Inventory row.
    kind: OpKindRef,
    /// Verified class.
    class: WriteClass,
    /// Canonical `ensure_ascii` body bytes.
    body: Arc<[u8]>,
    /// Operation decoded off the writer thread by [`WriteEnvelope::predecode`].
    decoded: Option<Arc<RemoteOp>>,
}

impl WriteEnvelope {
    /// Builds an envelope for a remote operation, validating the producer,
    /// ordinal and decoded bound, and computing digest, class and body.
    pub fn new(
        operation_id: OperationId,
        producer: ProducerId,
        ordinal: u64,
        deadline: MonoDeadline,
        op: RemoteOp,
    ) -> Result<Self> {
        let kind = op.kind();
        let class = op.class();
        let op_value = serde_json::to_value(&op)?;
        let digest = checked_digest(&producer, ordinal, kind, &op_value)?;
        let header = EnvelopeHeader {
            protocol: WRITE_PROTOCOL,
            operation_id,
            producer,
            ordinal,
            deadline,
            digest,
        };
        let body = dumps(
            &serde_json::to_value(WireBody {
                header: header.clone(),
                op,
            })?,
            true,
        );
        if body.len() > kind.wire_max() {
            return Err(invalid("operation exceeds its wire bound"));
        }
        Ok(Self {
            header,
            kind,
            class,
            body: body.into(),
            decoded: None,
        })
    }

    /// Verifies one received body on the transport thread. `header_kind` is
    /// the wire id from the transport header and must name a remote row equal
    /// to the decoded operation kind; the body must fit that row's wire bound,
    /// use this protocol, decode into the closed [`RemoteOp`] set and carry
    /// the recomputed digest. Refusals are non-durable gate rejections.
    #[allow(clippy::result_large_err)] // the agreed API returns the refusal itself
    pub fn from_wire(header_kind: u16, body: &[u8]) -> std::result::Result<Self, StorageRejection> {
        let gate = |code, message: &str| StorageRejection::new(code, RejectStage::Gate, message);
        let kind = OpKindRef::from_wire_id(header_kind)
            .filter(|kind| kind.is_remote())
            .ok_or_else(|| gate(RejectCode::ProtocolUnsupported, "unknown operation kind"))?;
        if body.len() > kind.wire_max() {
            return Err(gate(
                RejectCode::PayloadTooLarge,
                "operation exceeds its wire bound",
            ));
        }
        let parsed: WireBody = serde_json::from_slice(body)
            .map_err(|_| gate(RejectCode::Validation, "malformed operation body"))?;
        if parsed.header.protocol != WRITE_PROTOCOL {
            return Err(gate(
                RejectCode::ProtocolUnsupported,
                "unsupported writer protocol",
            ));
        }
        if parsed.op.kind() != kind {
            return Err(gate(
                RejectCode::ProtocolUnsupported,
                "header kind does not match body",
            ));
        }
        let op_value = serde_json::to_value(&parsed.op)
            .map_err(|_| gate(RejectCode::Validation, "malformed operation body"))?;
        let digest = checked_digest(
            &parsed.header.producer,
            parsed.header.ordinal,
            kind,
            &op_value,
        )
        .map_err(|error| StorageRejection::from_error(&error, RejectStage::Gate))?;
        if digest != parsed.header.digest {
            return Err(gate(RejectCode::DigestMismatch, "payload digest mismatch"));
        }
        let class = parsed.op.class();
        Ok(Self {
            header: parsed.header,
            kind,
            class,
            body: body.into(),
            decoded: None,
        })
    }

    /// Wire id and canonical body for the transport.
    pub fn to_wire(&self) -> (u16, &[u8]) {
        (self.kind.wire_id(), &self.body)
    }

    /// Decodes the retained body back into the operation (dispatcher thread).
    pub fn decode(&self) -> Result<RemoteOp> {
        let parsed: WireBody = serde_json::from_slice(&self.body)?;
        Ok(parsed.op)
    }

    /// Decodes the body now (dispatcher thread) so the writer thread does not
    /// parse JSON; keeps the decoded operation with the envelope.
    pub fn predecode(&mut self) -> Result<()> {
        if self.decoded.is_none() {
            self.decoded = Some(Arc::new(self.decode()?));
        }
        Ok(())
    }

    /// Drops a predecoded operation, keeping only the canonical body (queued
    /// jobs retain no decoded graph).
    pub fn strip_decoded(&mut self) {
        self.decoded = None;
    }

    /// Whether a predecoded operation is held.
    pub fn is_decoded(&self) -> bool {
        self.decoded.is_some()
    }

    /// Header and operation, moving a predecoded operation out without a
    /// copy when this envelope holds it alone.
    pub(crate) fn into_parts(self) -> (EnvelopeHeader, Result<RemoteOp>) {
        let op = match self.decoded {
            Some(op) => Ok(Arc::try_unwrap(op).unwrap_or_else(|shared| (*shared).clone())),
            None => self.decode(),
        };
        (self.header, op)
    }

    /// The operation to execute: the predecoded one, else decoded now.
    pub(crate) fn operation(&self) -> Result<RemoteOp> {
        match &self.decoded {
            Some(op) => Ok((**op).clone()),
            None => self.decode(),
        }
    }

    /// Header fields.
    pub fn header(&self) -> &EnvelopeHeader {
        &self.header
    }

    /// Inventory row.
    pub fn kind(&self) -> OpKindRef {
        self.kind
    }

    /// Verified class.
    pub fn class(&self) -> WriteClass {
        self.class
    }

    /// Canonical encoded length (queue byte accounting).
    pub fn encoded_len(&self) -> usize {
        self.body.len()
    }
}

/// A broker-local operation submission. Fields are private; the only
/// constructor computes the digest and encoded size off the writer thread.
#[derive(Clone)]
pub struct LocalEnvelope {
    /// Header fields.
    header: EnvelopeHeader,
    /// Operation.
    op: LocalOp,
    /// Canonical encoded length.
    encoded_len: usize,
}

impl LocalEnvelope {
    /// Builds an envelope for a broker-local operation, validating producer,
    /// ordinal and the decoded bound.
    pub fn new(
        operation_id: OperationId,
        producer: ProducerId,
        ordinal: u64,
        deadline: MonoDeadline,
        op: LocalOp,
    ) -> Result<Self> {
        let op_value = serde_json::to_value(&op)?;
        let digest = checked_digest(&producer, ordinal, op.kind(), &op_value)?;
        let encoded_len = dumps(&op_value, true).len();
        Ok(Self {
            header: EnvelopeHeader {
                protocol: WRITE_PROTOCOL,
                operation_id,
                producer,
                ordinal,
                deadline,
                digest,
            },
            op,
            encoded_len,
        })
    }

    /// Header fields.
    pub fn header(&self) -> &EnvelopeHeader {
        &self.header
    }

    /// Operation.
    pub fn op(&self) -> &LocalOp {
        &self.op
    }

    /// Records the broker's pre-SQL launch-secret verdict on a finish
    /// callback. The verdict is not serialized, so the envelope's digest,
    /// encoded length and receipt identity are unchanged; other operations
    /// ignore it.
    pub fn set_finish_guard(&mut self, guard: agent_run_domain::worker::FinishGuard) {
        if let LocalOp::AcceptWorkerFinish(op) = &mut self.op {
            op.guard = guard;
        }
    }

    /// Header and operation, moved out of the envelope.
    pub(crate) fn into_parts(self) -> (EnvelopeHeader, LocalOp) {
        (self.header, self.op)
    }

    /// Inventory row.
    pub fn kind(&self) -> OpKindRef {
        self.op.kind()
    }

    /// Verified class.
    pub fn class(&self) -> WriteClass {
        self.op.class()
    }

    /// Canonical encoded length (queue byte accounting).
    pub fn encoded_len(&self) -> usize {
        self.encoded_len
    }
}

/// Typed refusal for paths not implemented yet; never a committed success.
pub fn unsupported(kind: OpKindRef) -> StorageRejection {
    StorageRejection::new(
        RejectCode::Unsupported,
        RejectStage::Gate,
        format!("operation {} is not supported by this writer", kind.name()),
    )
}

/// Physical maintenance job run by the writer between business transactions
/// and outside any transaction, without receipt or replay. Broker only; never
/// decodable from a request frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "job", rename_all = "snake_case")]
pub enum MaintenanceOp {
    /// `wal_checkpoint`, PASSIVE or TRUNCATE; reports (busy, log, checkpointed).
    CheckpointWal {
        /// Use TRUNCATE instead of PASSIVE.
        truncate: bool,
    },
    /// `incremental_vacuum` of at most `pages` (<= 1024) pages.
    VacuumIncremental {
        /// Page bound.
        pages: u16,
    },
}
