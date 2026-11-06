//! Portable route configuration for account-owned launch workers.

use agent_run_core::system_launch::SystemRoute;
use agent_run_domain::{
    Result,
    catalog::{HarnessId, ProviderId},
    error::invalid,
};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Component, Path, PathBuf},
};

/// Socket and private Agent Run home of a worker already running as its account.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Worker {
    socket: PathBuf,
    private_home: PathBuf,
}

/// Shared broker endpoints and the native harness-to-worker mapping.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Group-accessible operator endpoint.
    pub operator_socket: PathBuf,
    /// Group-accessible supervisor writer endpoint.
    pub writer_socket: PathBuf,
    /// Additional operator endpoints governed by their host directory groups.
    #[serde(default)]
    pub operator_sockets: Vec<PathBuf>,
    workers: BTreeMap<HarnessId, Worker>,
}

impl Config {
    /// Loads a bounded route file. The host controls access to it and its sockets.
    pub fn load(path: &Path) -> Result<Self> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(65_537)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 65_536 {
            return Err(invalid("worker route configuration is too large"));
        }
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| invalid("worker route configuration is not UTF-8"))?;
        let config: Self =
            toml::from_str(text).map_err(|_| invalid("worker route configuration is invalid"))?;
        if config.workers.is_empty()
            || config.operator_sockets.len() > 16
            || !absolute(&config.operator_socket)
            || !absolute(&config.writer_socket)
            || config.operator_socket == config.writer_socket
            || config.operator_sockets.iter().any(|path| !absolute(path))
            || config
                .operator_sockets
                .iter()
                .any(|path| path == &config.operator_socket || path == &config.writer_socket)
            || config
                .operator_sockets
                .iter()
                .enumerate()
                .any(|(index, path)| config.operator_sockets[..index].contains(path))
            || config
                .workers
                .values()
                .any(|worker| !absolute(&worker.socket) || !absolute(&worker.private_home))
        {
            return Err(invalid("worker routes require distinct absolute endpoints"));
        }
        Ok(config)
    }

    /// Freezes the selected worker and its private home into one admission.
    pub fn route(
        &self,
        home: &Path,
        provider: &ProviderId,
        harness: HarnessId,
    ) -> Result<SystemRoute> {
        let worker = self
            .workers
            .get(&harness)
            .ok_or_else(|| invalid("provider harness has no launch worker"))?;
        if !absolute(home) {
            return Err(invalid("shared state home must be absolute"));
        }
        let fingerprint = agent_run_platform::fs::sha256(&serde_json::to_vec(&(
            provider,
            harness,
            home,
            &worker.socket,
            &worker.private_home,
            &self.operator_socket,
            &self.writer_socket,
        ))?);
        let route = SystemRoute {
            provider: provider.clone(),
            harness,
            state_home: home.to_path_buf(),
            private_home: worker.private_home.clone(),
            operator_socket: self.operator_socket.clone(),
            writer_socket: self.writer_socket.clone(),
            worker_socket: worker.socket.clone(),
            fingerprint: fingerprint.parse()?,
        };
        route.validate_for(provider, harness)?;
        Ok(route)
    }
}

/// Rejects relative paths and parent traversal; OS permissions govern access.
fn absolute(path: &Path) -> bool {
    path.is_absolute()
        && !path
            .components()
            .any(|part| matches!(part, Component::ParentDir))
}
