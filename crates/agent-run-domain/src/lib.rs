//! Validated product types and transport-safe errors.
pub mod canonical;
pub mod catalog;
pub mod credential_ref;
pub mod domain;
pub mod error;
pub mod fsm;
mod legacy_provider;
mod provider_connection;
pub mod provider_start;
pub mod quota_snapshot;
pub mod tools;
pub mod types;
pub mod views;

pub use catalog::{
    AccountId, AccountRecord, AccountStatus, AttemptCredentials, AuthFamily, CredentialHeader,
    HarnessId, LimitsSource, NormalizedQuotaSnapshot, PhysicalQuotaKey, ProviderBinding,
    ProviderCatalog, ProviderConnection, ProviderDefinition, ProviderId, ProviderModel,
    ProviderProtocol, QuotaAdmissionError, QuotaCandidate, QuotaCandidateSet,
    QuotaModelObservation, QuotaPoolObservation, QuotaWindow, ResolvedLaunchAuthority,
    SecretHandle, SecretRef, SelectionIntent,
};
pub use credential_ref::CredentialRef;
pub use error::{Error, MachineCode, ProtocolMapping, PublicError, Result};
pub use fsm::{validate_transition, ACTIVE, TERMINAL};
pub use provider_start::ProviderStartRequest;
pub use tools::{
    is_tool, registry, tool, tools_json, ArgumentDefault, ArgumentDefinition, ToolDefinition,
};
pub use types::{
    AbsoluteDirectory, AccountLabel, AccountSelector, GlobalAccount, NonNegativeFinite,
    PositiveFinite, ProcessBirthProof, RelativeOwnedPath, RuntimeName, Sha256Digest,
};
pub use views::{
    AgentPage, AgentView, AnswerView, CleanupView, CommandView, DeliveryView, MessageView,
    StartResult, TranscriptPage,
};
