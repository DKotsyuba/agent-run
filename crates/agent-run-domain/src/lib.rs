//! Validated product types and transport-safe errors.
/// Private attempt-binding data, inactive until authenticated routing is enabled.
pub mod attempt_binding;
/// Generic durable batch admission contracts.
pub mod batch;
pub mod bridge;
pub mod canonical;
pub mod catalog;
pub mod credential_ref;
pub mod domain;
/// Private Pool-resident managed enrollment contracts.
pub mod enrollment;
pub mod error;
pub mod fsm;
/// Frozen ping policy and private model response contracts.
pub mod health;
pub mod initial_task;
pub mod integration;
mod legacy_provider;
/// Draft byte codec for future generic lineage holds; no hold is admitted here.
pub mod lineage_hold;
/// Draft strict JSON contracts for the future operator lineage-hold socket.
pub mod lineage_hold_wire;
pub mod observation;
/// Strict managed native-owner v2 outbox and request contracts.
pub mod owner_v2;
pub mod pool;
mod provider_connection;
pub mod provider_start;
/// Draft bounded database-only execution status proofs.
pub mod status;
/// Protocol types of the single resident SQLite writer.
pub mod storage;
pub mod tools;
/// Shared transcript bounds and representation options.
pub mod transcript;
pub mod transcript_exact;
pub mod types;
pub mod views;
pub mod worker;

pub use catalog::{
    AccountId, AccountRecord, AccountStatus, AttemptCredentials, AuthFamily, CredentialHeader,
    HarnessId, LimitsSource, PhysicalQuotaKey, ProviderBinding, ProviderCatalog,
    ProviderConnection, ProviderDefinition, ProviderId, ProviderModel, ProviderProtocol,
    QuotaAdmissionError, QuotaCandidate, QuotaCandidateSet, ResolvedLaunchAuthority, SecretHandle,
    SecretRef, SelectionIntent,
};
pub use credential_ref::CredentialRef;
pub use error::{
    Error, MachineCode, OwnershipStage, PreconditionFailureReason, ProtocolMapping, PublicError,
    Result,
};
pub use fsm::{ACTIVE, TERMINAL, validate_transition};
pub use provider_start::ProviderStartRequest;
pub use tools::{
    ArgumentDefault, ArgumentDefinition, ToolDefinition, is_tool, registry, tool, tools_json,
};
pub use types::{
    AbsoluteDirectory, AccountLabel, AccountSelector, GlobalAccount, NonNegativeFinite,
    PositiveFinite, ProcessBirthProof, RelativeOwnedPath, RuntimeName, Sha256Digest, StoreId,
};
pub use views::{
    AgentPage, AgentView, AnswerView, CapacityOrderQuery, CleanupView, CommandView, DeliveryView,
    ListPoolsView, MessageView, ModelsQuery, PoolListMemberView, PoolListView, StartResult,
    TranscriptPage,
};
pub use worker::{NotifyReceipt, NotifyRequest, WorkerCall, WorkerMessageKind, WorkerNotice};
