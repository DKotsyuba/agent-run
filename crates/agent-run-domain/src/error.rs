use serde::Serialize;

pub type Result<T> = std::result::Result<T, Error>;

/// Stable public machine codes carried by CLI, JSON-RPC, and MCP error envelopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum MachineCode {
    /// The caller supplied an invalid public value.
    ValidationError,
    /// A derived path escaped its declared owned root.
    PathEscapeError,
    /// The requested durable agent does not exist.
    AgentNotFound,
    /// The requested lifecycle transition is not in the frozen state machine.
    StateTransitionError,
    /// A replay identity conflicts with an admitted request.
    RequestConflict,
    /// Admission cannot reserve another active agent slot.
    CapacityExhausted,
    /// A valid request asks for a feature unavailable in this installation.
    Unsupported,
    /// The resident broker cannot be reached.
    BrokerUnavailable,
    /// Answer evidence is malformed, missing, oversized, or inconsistent.
    AnswerIntegrityError,
    /// A safe runtime failure that has no more specific public category.
    RuntimeError,
    /// Local filesystem or descriptor I/O failed without exposing the source message.
    IOError,
    /// Durable state storage failed without exposing the database source message.
    StorageError,
}

impl MachineCode {
    /// Returns the exact Python-compatible public error type name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ValidationError => "ValidationError",
            Self::PathEscapeError => "PathEscapeError",
            Self::AgentNotFound => "AgentNotFound",
            Self::StateTransitionError => "StateTransitionError",
            Self::RequestConflict => "RequestConflict",
            Self::CapacityExhausted => "CapacityExhausted",
            Self::Unsupported => "Unsupported",
            Self::BrokerUnavailable => "BrokerUnavailable",
            Self::AnswerIntegrityError => "AnswerIntegrityError",
            Self::RuntimeError => "RuntimeError",
            Self::IOError => "IOError",
            Self::StorageError => "StorageError",
        }
    }
}

/// Protocol-specific error mapping derived only from a stable machine code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProtocolMapping {
    /// JSON-RPC error number used by the Unix socket transport.
    pub json_rpc_code: i32,
    /// MCP structured-error code.
    pub mcp_code: &'static str,
    /// CLI exit status for an expected product error.
    pub cli_exit_code: i32,
}

/// Expected domain failures; source-bearing variants never expose source diagnostics publicly.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A public input contract was violated.
    #[error("{0}")]
    Validation(String),
    /// The requested agent id was not found.
    #[error("unknown agent: {0}")]
    NotFound(String),
    /// A lifecycle transition is not permitted.
    #[error("invalid agent transition: {0}")]
    Transition(String),
    /// A path escaped the declared owned filesystem root.
    #[error("{0}")]
    PathEscape(String),
    /// A durable request identity conflicts with a prior admission.
    #[error("request identity conflicts with a previously admitted request")]
    Conflict,
    /// No active-agent capacity remains.
    #[error("agent capacity is exhausted")]
    Capacity,
    /// A requested supported-contract feature is unavailable.
    #[error("{0}")]
    Unsupported(String),
    /// The resident broker is unavailable.
    #[error("BrokerUnavailable: start `agent-run api serve` for this home")]
    BrokerUnavailable,
    /// Verified-answer evidence failed an integrity contract and is never validation.
    #[error("{0}")]
    AnswerIntegrity(String),
    /// Legacy integrity name retained temporarily for existing migration callers.
    #[error("{0}")]
    Integrity(String),
    /// A safe runtime error message.
    #[error("{0}")]
    Runtime(String),
    /// A detached supervisor failed before ownership handoff with bounded evidence.
    #[error("{message}")]
    Bootstrap {
        /// Agent admitted before supervisor bootstrap failed.
        agent_id: String,
        /// Stable bootstrap failure category.
        failure_kind: String,
        /// Optional named child bootstrap stage.
        failure_stage: Option<String>,
        /// Secret-safe failure summary.
        message: String,
    },
    /// A source-bearing local I/O failure whose public rendering is generic.
    #[error("{0}")]
    Io(#[from] std::io::Error),
    /// A temporary compatibility source at the SQLite boundary.
    ///
    /// Store migration removes this variant once all store entry points own their source mapping.
    #[error("{0}")]
    Sql(#[from] rusqlite::Error),
    /// Invalid JSON from a public boundary.
    #[error("{0}")]
    Json(#[from] serde_json::Error),
}

/// A bounded secret-safe public error envelope.
#[derive(Debug, Serialize)]
pub struct PublicError {
    /// Stable Python-compatible machine code.
    pub kind: &'static str,
    /// Human-readable, bounded, secret-safe message.
    pub message: String,
    /// Agent associated with a detached bootstrap failure, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// Bootstrap failure category, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_kind: Option<String>,
    /// Bootstrap failure stage, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_stage: Option<String>,
}
impl Error {
    /// Returns the stable machine code without formatting untrusted source diagnostics.
    pub const fn machine_code(&self) -> MachineCode {
        match self {
            Self::Validation(_) | Self::Json(_) => MachineCode::ValidationError,
            Self::PathEscape(_) => MachineCode::PathEscapeError,
            Self::NotFound(_) => MachineCode::AgentNotFound,
            Self::Transition(_) => MachineCode::StateTransitionError,
            Self::Conflict => MachineCode::RequestConflict,
            Self::Capacity => MachineCode::CapacityExhausted,
            Self::Unsupported(_) => MachineCode::Unsupported,
            Self::BrokerUnavailable => MachineCode::BrokerUnavailable,
            Self::AnswerIntegrity(_) | Self::Integrity(_) => MachineCode::AnswerIntegrityError,
            Self::Runtime(_) => MachineCode::RuntimeError,
            Self::Bootstrap { .. } => MachineCode::ValidationError,
            Self::Io(_) => MachineCode::IOError,
            Self::Sql(_) => MachineCode::StorageError,
        }
    }

    /// Returns JSON-RPC, MCP, and CLI mappings shared by every public transport.
    pub const fn protocol_mapping(&self) -> ProtocolMapping {
        let code = self.machine_code();
        ProtocolMapping {
            json_rpc_code: if matches!(
                code,
                MachineCode::ValidationError | MachineCode::PathEscapeError
            ) {
                -32602
            } else {
                -32000
            },
            mcp_code: code.as_str(),
            cli_exit_code: 2,
        }
    }

    /// Builds the bounded public envelope without carrying parser, database, or OS details.
    pub fn public(&self) -> PublicError {
        let message = match self {
            Self::Validation(s)
            | Self::Unsupported(s)
            | Self::AnswerIntegrity(s)
            | Self::Integrity(s)
            | Self::Runtime(s) => s.clone(),
            Self::Bootstrap { message, .. } => message.clone(),
            // Parser/OS/database diagnostics can contain configured secret values.
            Self::Io(_) => "local I/O operation failed".into(),
            Self::Sql(_) => "state database operation failed".into(),
            Self::Json(_) => "invalid JSON document".into(),
            _ => self.to_string(),
        };
        PublicError {
            kind: self.machine_code().as_str(),
            message: message.chars().take(512).collect(),
            agent_id: match self {
                Self::Bootstrap { agent_id, .. } => Some(agent_id.clone()),
                _ => None,
            },
            failure_kind: match self {
                Self::Bootstrap { failure_kind, .. } => Some(failure_kind.clone()),
                _ => None,
            },
            failure_stage: match self {
                Self::Bootstrap { failure_stage, .. } => failure_stage.clone(),
                _ => None,
            },
        }
    }
    /// Returns the socket JSON-RPC code from the shared protocol mapping.
    pub fn rpc_code(&self) -> i32 {
        self.protocol_mapping().json_rpc_code
    }
}
/// Constructs a typed public validation error from a safe category message.
pub fn invalid(message: impl Into<String>) -> Error {
    Error::Validation(message.into())
}
