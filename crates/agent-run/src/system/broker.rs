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

/// Create a fresh execution directory, or reuse its anchored directory only
/// for the exact prepared successor of an open maintenance recovery.
fn prepare_agent_directory(home: &std::path::Path, execution: &AgentId) -> Result<()> {
    let agents = home.join("agents");
    let state = fs::Dir::open_searchable(home)?;
    let parent = state.subdir(std::path::Path::new("agents"))?;
    let directory = agents.join(execution.as_str());
    match std::fs::DirBuilder::new().mode(0o770).create(&directory) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let reader = agent_run_store::writer::StoreReader::open(home)?;
            let continuing: bool = reader.conn().query_row(
                "SELECT EXISTS(SELECT 1 FROM maintenance_resumes r JOIN maintenance_fences f ON f.epoch=r.epoch \
                 JOIN maintenance_cohort c ON c.epoch=r.epoch AND c.agent_id=r.agent_id \
                 JOIN agents a ON a.id=r.agent_id JOIN attempts t ON t.id=r.target_attempt_id AND t.agent_id=a.id \
                 WHERE r.agent_id=? AND f.ended_at IS NULL AND f.phase IN ('recovering','deferring') \
                 AND a.sequence=c.sequence AND a.status='starting' AND a.finished_at IS NULL AND a.supervisor_pid IS NULL \
                 AND a.supervisor_capability_sha256 IS NULL AND t.state='prepared' AND t.phase='prepared' \
                 AND t.ownership_active=1 AND t.finished_at IS NULL)",
                [execution.as_str()], |row| row.get(0),
            )?;
            if !continuing {
                return Err(error.into());
            }
            let existing = parent.subdir(std::path::Path::new(execution.as_str()))?;
            if existing.entry(None)?.uid != state.entry(None)?.uid {
                return Err(Error::Integrity(
                    "maintenance agent directory owner changed".into(),
                ));
            }
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

impl SystemLauncher for BrokerLauncher {
    /// Resolves the configured worker for this provider harness.
    fn route(&self, provider: &ProviderId, harness: HarnessId) -> Result<SystemRoute> {
        self.config.route(&self.home, provider, harness)
    }

    /// Runs the private-file preflight under the selected worker's UID.
    fn preflight_resume(
        &self,
        parent: AgentId,
        route: SystemRoute,
    ) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'static>> {
        Box::pin(async move { super::client::preflight_resume(&parent, &route).await })
    }

    /// Keep maintenance private-file inspection under the frozen worker UID.
    fn preflight_maintenance_resume(
        &self,
        request: agent_run_core::system_launch::MaintenancePreflightRequest,
        route: SystemRoute,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<agent_run_core::system_launch::MaintenancePreflightResult>>
                + Send
                + 'static,
        >,
    > {
        Box::pin(async move { super::client::preflight_maintenance_resume(request, &route).await })
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
            prepare_agent_directory(&home, &execution)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A directory is reusable only for the exact parked execution's prepared
    /// successor; existing content stays intact and symlinks are rejected.
    #[test]
    fn maintenance_reuses_only_its_owned_agent_directory() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        let mut store = agent_run_store::fixture::initialize(&home).unwrap();
        std::fs::create_dir(home.join("agents")).unwrap();
        let id: AgentId = "ag-20261105-120000-1234567890".parse().unwrap();
        store.conn.execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,root_agent_id) VALUES(?,'mock','fixture','review','task','task','/fixture','{}','starting',1,'fixture',?)",
            rusqlite::params![id.as_str(),id.as_str()],
        ).unwrap();
        prepare_agent_directory(&home, &id).unwrap();
        let directory = home.join("agents").join(id.as_str());
        std::fs::write(directory.join("retained"), b"existing").unwrap();
        assert!(prepare_agent_directory(&home, &id).is_err());
        let epoch = store
            .begin_maintenance_fence("directory-recovery", 2.0)
            .unwrap()
            .epoch;
        store.defer_maintenance_fence(epoch).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,phase,adapter_state_json,created_at,ownership_active) VALUES('directory-target',?,2,'prepared','prepared','{}',2,1)", [id.as_str()]).unwrap();
        store.conn.execute("INSERT INTO maintenance_resumes(epoch,agent_id,request_id,source_attempt_id,target_attempt_id,target_number,account_id,source_proof_sha256,writer_epoch,claimed_at,preflight_json) VALUES(?,?,'directory-resume','source','directory-target',2,'fixture',?,1,2,'{}')",rusqlite::params![epoch,id.as_str(),"a".repeat(64)]).unwrap();
        prepare_agent_directory(&home, &id).unwrap();
        assert_eq!(
            std::fs::read(directory.join("retained")).unwrap(),
            b"existing"
        );
        std::fs::remove_file(directory.join("retained")).unwrap();
        std::fs::remove_dir(&directory).unwrap();
        std::os::unix::fs::symlink(&home, &directory).unwrap();
        assert!(prepare_agent_directory(&home, &id).is_err());
    }
}
