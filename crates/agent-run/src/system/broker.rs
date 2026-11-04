//! Broker adapter for unprivileged, account-owned launch workers.

use super::{Client, ClientError, Config};
use agent_run_core::{
    broker_writer::{BROKER_BUDGET, BrokerLane},
    system_launch::{SystemLauncher, SystemRoute},
};
use agent_run_domain::{
    Error, Result,
    catalog::{HarnessId, ProviderId},
    domain::AgentId,
    error::invalid,
};
use agent_run_platform::{fs, process};
use agent_run_store::ops::{RecordStartupOwner, RecordSupervisorLaunch, SystemStartupFence};
use std::{
    future::Future,
    os::unix::{fs::DirBuilderExt, net::UnixStream},
    path::PathBuf,
    pin::Pin,
};

/// Routes each provider to the worker already running as its native account.
pub struct BrokerLauncher {
    home: PathBuf,
    config: Config,
    broker: process::Identity,
}

impl BrokerLauncher {
    /// Captures the broker identity used to fence each durable handoff.
    pub fn new(home: PathBuf, config: Config) -> Result<Self> {
        Ok(Self {
            home,
            config,
            broker: process::inspect(std::process::id() as i32)?,
        })
    }
}

impl SystemLauncher for BrokerLauncher {
    /// Resolves the configured worker for this provider harness.
    fn route(&self, provider: &ProviderId, harness: HarnessId) -> Result<SystemRoute> {
        self.config.route(&self.home, provider, harness)
    }

    /// Records the launch, requests one supervisor, and releases its capability
    /// only after the broker and frozen-route ownership fence is durable.
    fn launch(
        &self,
        home: PathBuf,
        execution: AgentId,
        route: SystemRoute,
        lane: BrokerLane,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        let broker = self.broker.clone();
        Box::pin(async move {
            if home != route.state_home {
                return Err(invalid("worker route state home does not match the broker"));
            }
            // The host configures group, ownership and umask on the parent.
            let agents = home.join("agents");
            fs::Dir::open(&agents)?;
            std::fs::DirBuilder::new()
                .mode(0o770)
                .create(agents.join(execution.as_str()))?;
            let secret = format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            );
            let capability_sha256 = fs::sha256(secret.as_bytes());
            lane.local(
                RecordSupervisorLaunch {
                    execution: execution.clone(),
                    capability_sha256: capability_sha256.clone(),
                },
                BROKER_BUDGET,
            )
            .await?;
            let socket = route.worker_socket.clone();
            let request_id = execution.clone();
            let request_hash = capability_sha256.clone();
            let pending = tokio::task::spawn_blocking(move || {
                let stream = UnixStream::connect(socket).map_err(ClientError::BeforeSend)?;
                Client::connect(stream)?.start(&request_id, &request_hash)
            })
            .await
            .map_err(|error| Error::Runtime(format!("launch worker failed: {error}")))?
            .map_err(launch_error)?;
            let mut identity = serde_json::to_value(pending.identity())?;
            identity["spawn_backend"] = serde_json::to_value(pending.backend())?;
            let operation = RecordStartupOwner {
                execution,
                identity,
                system_fence: Some(SystemStartupFence {
                    capability_sha256,
                    route_fingerprint: route.fingerprint.as_str().to_owned(),
                    broker_identity: serde_json::to_value(broker)?,
                }),
            };
            // A pending commit remains owned even if the launch deadline expires.
            let record = tokio::spawn(async move { lane.local(operation, BROKER_BUDGET).await });
            pending
                .record_and_release(secret, async move {
                    record
                        .await
                        .map_err(std::io::Error::other)?
                        .map_err(std::io::Error::other)
                })
                .await
                .map_err(launch_error)
        })
    }
}

/// Keep pre-send refusals distinct from uncertain post-dispatch outcomes.
fn launch_error(error: ClientError) -> Error {
    match error {
        ClientError::BeforeSend(source) => Error::Io(source),
        ClientError::Refused(reason) => Error::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            reason,
        )),
        other => Error::Runtime(format!("launch worker outcome is uncertain: {other}")),
    }
}
