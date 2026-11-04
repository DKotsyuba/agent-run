//! Broker half of the one-shot launch handshake.

use super::wire::{self, Backend, Release, Reply, Start};
use agent_run_domain::domain::AgentId;
use agent_run_platform::{
    fs,
    process::{self, Identity},
};
use std::{
    error::Error as StdError,
    fmt,
    future::Future,
    io,
    os::unix::net::UnixStream,
    time::{Duration, Instant},
};

/// Failure classes for a launch; none authorizes retrying the same execution.
#[derive(Debug)]
pub enum ClientError {
    /// The connection or request was rejected before Start was sent.
    BeforeSend(io::Error),
    /// The coordinator refused before consuming a launch claim.
    Refused(String),
    /// The durable startup-owner operation failed; closing the gate triggers cleanup.
    OwnerNotRecorded(Box<dyn StdError + Send + Sync>),
    /// Start may have been claimed or released; reconcile without relaunching.
    Unknown(io::Error),
}

impl fmt::Display for ClientError {
    /// Describe the decision point without rendering a capability.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeSend(error) => write!(f, "coordinator request was not sent: {error}"),
            Self::Refused(reason) => write!(f, "coordinator refused launch: {reason}"),
            Self::OwnerNotRecorded(error) => write!(f, "startup owner was not recorded: {error}"),
            Self::Unknown(error) => write!(f, "coordinator launch outcome is unknown: {error}"),
        }
    }
}

impl StdError for ClientError {
    /// Preserve local I/O and durable-owner causes for broker diagnostics.
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::BeforeSend(error) | Self::Unknown(error) => Some(error),
            Self::OwnerNotRecorded(error) => Some(error.as_ref()),
            Self::Refused(_) => None,
        }
    }
}

/// One connection to an account-owned worker, consumed by the first request.
pub struct Client {
    stream: UnixStream,
}

/// A claimed child held behind its unreleased capability pipe.
pub struct PendingLaunch {
    stream: UnixStream,
    identity: Identity,
    backend: Backend,
    capability_sha256: String,
    deadline: Instant,
}

impl Client {
    /// Uses a worker socket whose access is configured by the operating system.
    pub fn connect(stream: UnixStream) -> Result<Self, ClientError> {
        stream
            .set_read_timeout(Some(Duration::from_secs(45)))
            .map_err(ClientError::BeforeSend)?;
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .map_err(ClientError::BeforeSend)?;
        Ok(Self { stream })
    }

    /// Dispatch one already-durably-claimed execution and receive its real PID/birth.
    ///
    /// Admission must freeze `authority` and the capability hash before calling.
    /// A socket failure after beginning the write always means unknown outcome.
    pub fn start(
        mut self,
        execution: &AgentId,
        capability_sha256: &str,
    ) -> Result<PendingLaunch, ClientError> {
        if !wire::lowercase_hex64(capability_sha256) {
            return Err(ClientError::BeforeSend(invalid(
                "invalid frozen launch authority",
            )));
        }
        let start = Start {
            execution: execution.clone(),
            capability_sha256: capability_sha256.to_owned(),
        };
        // Starting the budget before dispatch is conservative relative to the
        // coordinator's later spawn-to-READY budget.
        let deadline = Instant::now() + agent_run_platform::launch::READY_TIMEOUT;
        wire::write_until(
            &mut self.stream,
            &start,
            deadline.min(Instant::now() + Duration::from_secs(5)),
        )
        .map_err(ClientError::Unknown)?;
        match wire::read_until(&mut self.stream, deadline).map_err(ClientError::Unknown)? {
            Reply::Spawned { identity, backend }
                if identity.pid > 1
                    && identity.group == identity.pid
                    && !identity.zombie
                    && identity.birth.is_finite()
                    && identity.birth >= 0.0
                    && (identity.token.starts_with("linux:")
                        || identity.token.starts_with("darwin:")) =>
            {
                let actual = process::inspect(identity.pid).map_err(ClientError::Unknown)?;
                if actual.token != identity.token
                    || actual.birth != identity.birth
                    || actual.group != identity.group
                    || actual.zombie
                {
                    return Err(ClientError::Unknown(invalid(
                        "worker reported a different process identity",
                    )));
                }
                Ok(PendingLaunch {
                    stream: self.stream,
                    identity,
                    backend,
                    capability_sha256: capability_sha256.to_owned(),
                    deadline,
                })
            }
            Reply::Refused { reason } => Err(ClientError::Refused(reason)),
            Reply::Unknown { reason } => Err(ClientError::Unknown(io::Error::other(reason))),
            _ => Err(ClientError::Unknown(invalid(
                "unexpected coordinator reply",
            ))),
        }
    }
}

impl PendingLaunch {
    /// Construct a pending launch around the real launcher fixture in unit tests.
    #[cfg(test)]
    pub(super) fn fixture(
        stream: UnixStream,
        identity: Identity,
        backend: Backend,
        capability_sha256: String,
    ) -> Self {
        Self {
            stream,
            identity,
            backend,
            capability_sha256,
            deadline: Instant::now() + agent_run_platform::launch::READY_TIMEOUT,
        }
    }

    /// Kernel identity to include in the durable RecordStartupOwner operation.
    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    /// Platform spawn backend to include with startup evidence.
    pub fn backend(&self) -> Backend {
        self.backend
    }

    /// Await the broker's durable owner commit before releasing the capability.
    /// Dropping this future while awaiting the commit closes the stream and
    /// triggers verified cleanup; after release, an unknown result is not retried.
    pub async fn record_and_release<F, E>(
        self,
        capability: String,
        record: F,
    ) -> Result<(), ClientError>
    where
        F: Future<Output = Result<(), E>>,
        E: StdError + Send + Sync + 'static,
    {
        if !wire::lowercase_hex64(&capability)
            || fs::sha256(capability.as_bytes()) != self.capability_sha256
        {
            return Err(ClientError::Unknown(invalid(
                "capability does not match the recorded hash",
            )));
        }
        tokio::time::timeout_at(tokio::time::Instant::from_std(self.deadline), record)
            .await
            .map_err(|_| {
                ClientError::Unknown(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "startup owner commit exceeded the launch budget",
                ))
            })?
            .map_err(|error| ClientError::OwnerNotRecorded(Box::new(error)))?;
        tokio::task::spawn_blocking(move || self.release(capability))
            .await
            .map_err(|error| ClientError::Unknown(io::Error::other(error)))?
    }

    /// Transfer the capability once and wait for the real READY handshake.
    fn release(mut self, capability: String) -> Result<(), ClientError> {
        wire::write_until(
            &mut self.stream,
            &Release { capability },
            self.deadline.min(Instant::now() + Duration::from_secs(5)),
        )
        .map_err(ClientError::Unknown)?;
        match wire::read_until(&mut self.stream, self.deadline).map_err(ClientError::Unknown)? {
            Reply::Ready => Ok(()),
            Reply::Unknown { reason } => Err(ClientError::Unknown(io::Error::other(reason))),
            _ => Err(ClientError::Unknown(invalid("unexpected release reply"))),
        }
    }
}

/// Produce a nonsecret invalid-protocol error.
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
