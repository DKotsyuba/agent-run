//! Frozen system routes and the broker's alternate supervisor launch boundary.

use crate::{Result, broker_writer::BrokerLane};
use agent_run_domain::{
    Sha256Digest,
    catalog::{HarnessId, ProviderId},
    domain::AgentId,
    error::invalid,
};
use serde::{Deserialize, Serialize};
use std::{future::Future, pin::Pin};

/// A configured worker route captured before durable admission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemRoute {
    /// Provider matched to the admitted request.
    pub provider: ProviderId,
    /// Harness matched to the frozen provider authority.
    pub harness: HarnessId,
    /// Shared state home.
    pub state_home: std::path::PathBuf,
    /// Execution account's private Agent Run home for credentials and assets.
    pub private_home: std::path::PathBuf,
    /// Shared operator endpoint used by worker clients.
    pub operator_socket: std::path::PathBuf,
    /// Shared supervisor writer endpoint.
    pub writer_socket: std::path::PathBuf,
    /// Socket of the launcher already running as the desired account.
    pub worker_socket: std::path::PathBuf,
    /// Digest of the frozen route and supervisor configuration.
    pub fingerprint: Sha256Digest,
}

impl SystemRoute {
    /// Refuses nonabsolute paths or a provider/harness mismatch before admission.
    pub fn validate_for(&self, provider: &ProviderId, harness: HarnessId) -> Result<()> {
        if [
            &self.state_home,
            &self.private_home,
            &self.operator_socket,
            &self.writer_socket,
            &self.worker_socket,
        ]
        .iter()
        .any(|path| {
            !path.is_absolute()
                || path
                    .components()
                    .any(|part| matches!(part, std::path::Component::ParentDir))
        }) || &self.provider != provider
            || self.harness != harness
        {
            return Err(invalid("system route does not match provider authority"));
        }
        Ok(())
    }
}

/// A configured system launcher uses one frozen route and the broker writer.
///
/// Route lookup is nonblocking and reads only the trusted startup catalogue.
/// `launch` constructs a future without starting work; the service runs that
/// future in an owned task so client cancellation cannot abandon its handoff.
/// An I/O error may be returned only if no start request or child could have
/// been created; ambiguous results use another error so reconciliation retains ownership.
pub trait SystemLauncher: Send + Sync {
    /// Resolve one configured route, or refuse before durable admission.
    fn route(&self, provider: &ProviderId, harness: HarnessId) -> Result<SystemRoute>;

    /// Launch one already admitted execution with its frozen route.
    fn launch(
        &self,
        home: std::path::PathBuf,
        execution: AgentId,
        route: SystemRoute,
        lane: BrokerLane,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
}
