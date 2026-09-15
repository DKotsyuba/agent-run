//! Validated product types and transport-safe errors.
pub mod domain;
pub mod error;
pub mod fsm;
pub mod types;
pub mod views;

pub use error::{Error, MachineCode, ProtocolMapping, PublicError, Result};
pub use fsm::{validate_transition, ACTIVE, TERMINAL};
pub use types::{
    AbsoluteDirectory, AccountLabel, AccountSelector, GlobalAccount, NonNegativeFinite,
    PositiveFinite, ProcessBirthProof, RelativeOwnedPath, RuntimeName, Sha256Digest,
};
pub use views::{
    AgentPage, AgentView, AnswerView, CleanupView, CommandView, DeliveryView, MessageView,
    StartResult, TranscriptPage,
};
