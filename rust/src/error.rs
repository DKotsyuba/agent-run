use serde::Serialize;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Validation(String),
    #[error("unknown agent: {0}")]
    NotFound(String),
    #[error("invalid transition: {0}")]
    Transition(String),
    #[error("request identity conflicts with a previously admitted request")]
    Conflict,
    #[error("agent capacity is exhausted")]
    Capacity,
    #[error("{0}")]
    Unsupported(String),
    #[error("BrokerUnavailable: start `agent-run api serve` for this home")]
    BrokerUnavailable,
    #[error("{0}")]
    Integrity(String),
    #[error("{0}")]
    Runtime(String),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Sql(#[from] rusqlite::Error),
    #[error("{0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Serialize)]
pub struct PublicError {
    pub kind: &'static str,
    pub message: String,
}
impl Error {
    pub fn public(&self) -> PublicError {
        let (kind, message) = match self {
            Self::Validation(s) => ("ValidationError", s.clone()),
            Self::NotFound(_) => ("AgentNotFound", self.to_string()),
            Self::Transition(_) => ("StateTransitionError", self.to_string()),
            Self::Conflict => ("RequestConflict", self.to_string()),
            Self::Capacity => ("CapacityExhausted", self.to_string()),
            Self::Unsupported(s) => ("Unsupported", s.clone()),
            Self::BrokerUnavailable => ("BrokerUnavailable", self.to_string()),
            Self::Integrity(s) => ("AnswerIntegrityError", s.clone()),
            Self::Runtime(s) => ("RuntimeError", s.clone()),
            // Parser/OS/database diagnostics can contain configured secret values.
            Self::Io(_) => ("IOError", "local I/O operation failed".into()),
            Self::Sql(_) => ("StorageError", "state database operation failed".into()),
            Self::Json(_) => ("ValidationError", "invalid JSON document".into()),
        };
        PublicError { kind, message: message.chars().take(512).collect() }
    }
    pub fn rpc_code(&self) -> i32 {
        match self { Self::Validation(_) | Self::Json(_) => -32602, Self::BrokerUnavailable => -32001, _ => -32000 }
    }
}
pub fn invalid(message: impl Into<String>) -> Error { Error::Validation(message.into()) }
