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

/// Closed, content-free reasons why a system continuation could not be proved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumePreflightFailure {
    WorkerUnavailable,
    WorkerTimeout,
    AccessDenied,
    RouteChanged,
    ParentUnverifiable,
    ParentChanged,
    AssetsUnverifiable,
    HistoryUnverifiable,
}

impl ResumePreflightFailure {
    /// Stable nonsecret reason for the existing continuation error envelope.
    pub fn code(self) -> &'static str {
        match self {
            Self::WorkerUnavailable => "worker_unavailable",
            Self::WorkerTimeout => "worker_timeout",
            Self::AccessDenied => "access_denied",
            Self::RouteChanged => "route_changed",
            Self::ParentUnverifiable => "parent_unverifiable",
            Self::ParentChanged => "parent_changed",
            Self::AssetsUnverifiable => "assets_unverifiable",
            Self::HistoryUnverifiable => "history_unverifiable",
        }
    }
}

/// Broker-evaluated policy for the frozen eligible accounts, in their stored
/// order. No current configuration, paths, credential references or values
/// cross the group socket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum CurrentPolicy {
    /// The broker could not load its validated current configuration.
    Unavailable,
    /// The current route or policy no longer permits the frozen execution.
    Refused,
    /// Each bit corresponds to one frozen eligible account.
    Allowed {
        /// Digest of the frozen authority used to order these decisions.
        authority_sha256: String,
        /// Whether the current provider still binds this account and model.
        bound: Vec<bool>,
        /// Whether an ordered binding still names the frozen connection.
        unchanged: Vec<bool>,
    },
}

/// Reads a broker-evaluated policy over the existing shared operator socket.
/// The peer must be the state-home owner; failure is never an authorization.
pub async fn current_policy(
    home: &std::path::Path,
    route: &SystemRoute,
    id: &AgentId,
) -> Result<CurrentPolicy> {
    use std::os::unix::fs::MetadataExt;
    use tokio::io::BufReader;
    let uid = std::fs::metadata(home)?.uid();
    let operation = async {
        let stream = tokio::net::UnixStream::connect(&route.operator_socket).await?;
        if stream.peer_cred()?.uid() != uid {
            return Err(crate::Error::BrokerUnavailable);
        }
        let (input, mut output) = stream.into_split();
        let mut input = BufReader::new(input);
        agent_run_platform::frame::write(
            &mut output,
            &serde_json::json!({"jsonrpc":"2.0","id":1,"method":"_system_policy","params":{"execution":id}}),
            65_536,
        ).await?;
        let raw = agent_run_platform::frame::read(&mut input, 65_536)
            .await?
            .ok_or(crate::Error::BrokerUnavailable)?;
        let response: serde_json::Value = serde_json::from_slice(&raw)?;
        if response["jsonrpc"] != "2.0" || response["id"] != 1 {
            return Err(crate::Error::BrokerUnavailable);
        }
        serde_json::from_value(
            response
                .get("result")
                .cloned()
                .ok_or(crate::Error::BrokerUnavailable)?,
        )
        .map_err(Into::into)
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), operation)
        .await
        .map_err(|_| crate::Error::BrokerUnavailable)?
}

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

    /// Verify a terminal parent's private assets and native history under its
    /// frozen worker route, returning only a digest of the checked store rows.
    fn preflight_resume(
        &self,
        _parent: AgentId,
        _route: SystemRoute,
    ) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'static>> {
        Box::pin(async {
            Err(crate::Error::Unsupported(
                "continuation_unavailable: system preflight is unavailable".into(),
            ))
        })
    }

    /// Launch one already admitted execution with its frozen route.
    fn launch(
        &self,
        home: std::path::PathBuf,
        execution: AgentId,
        route: SystemRoute,
        lane: BrokerLane,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
}
