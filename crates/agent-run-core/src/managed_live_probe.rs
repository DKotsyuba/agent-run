//! Shared setup for the ignored live probes of the real managed launch path.
//!
//! `AGENT_RUN_LIVE_MANAGED_PROBE` names a JSON spec: `run_home` (a Run home,
//! read only) and per-harness sections `claude`/`codex` with `agent_id` (an
//! admitted managed attempt), `launch_dir` (its Run-generated runtime home),
//! `worker` (the `agent-run` build serving the worker MCP), `scratch` (an
//! empty private directory) and `evidence` (output file); Codex may name a
//! `models_cache` (the harness's own nonsecret model catalog) to seed. The frozen identity
//! and request come from the Run store, the role selection from the actual
//! selector, so the probes verify exactly what a supervisor would launch. No
//! credential is read; the worker capability is a dummy that never reaches a
//! broker, and Claude's API endpoint is a refused loopback port.
use agent_run_adapters::managed_role::Selection;
use agent_run_config::{provider_config::ProviderConfig, role_plan::ResolvedRolePlan};
use serde_json::Value;
use std::{collections::BTreeMap, path::PathBuf};

/// One frozen managed attempt and its live selection.
pub(crate) struct LiveManaged {
    pub run_home: PathBuf,
    pub section: Value,
    pub identity: crate::service::ProviderLaunchIdentity,
    pub request: crate::domain::StartRequest,
    pub role: ResolvedRolePlan,
    pub config: ProviderConfig,
    pub selection: Selection,
}

impl LiveManaged {
    pub fn path(&self, key: &str) -> PathBuf {
        PathBuf::from(self.section[key].as_str().expect("probe path"))
    }

    /// The worker MCP context the supervisor injects for a managed launch,
    /// with a dummy capability and an unreachable private endpoint: catalog
    /// registration fails closed and is retried, tools/list is unaffected.
    pub fn worker_environment(&self) -> BTreeMap<String, String> {
        let values = [
            "/nonexistent/agent-run-live-probe".to_owned(),
            self.section["agent_id"].as_str().unwrap().to_owned(),
            "att_live_probe".to_owned(),
            "0".repeat(64),
        ];
        let mut environment: BTreeMap<String, String> = agent_run_domain::worker::ENV_NAMES
            .into_iter()
            .map(str::to_owned)
            .zip(values)
            .collect();
        environment.insert(agent_run_domain::worker::FINISH_ENV.into(), "1".into());
        environment.insert(agent_run_domain::worker::RESEARCH_ENV.into(), "0".into());
        environment.insert(agent_run_domain::worker::MANAGED_ENV.into(), "1".into());
        environment
    }

    /// Materialize this attempt's native home into `home` with the build
    /// under test, exactly as a first launch does (`materialize_selected`),
    /// for the frozen role, the launch configuration and the account the
    /// attempt was admitted with. `app_home` receives any account link; the
    /// caller removes credentials before a harness reads the home.
    pub fn materialize(
        &self,
        home: &std::path::Path,
        work: &std::path::Path,
        app_home: &std::path::Path,
    ) {
        let db = rusqlite::Connection::open_with_flags(
            self.run_home.join("state.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap();
        let accounts = agent_run_store::accounts::list_at(&db).unwrap();
        let account: String = db
            .query_row(
                "SELECT selected_account_id FROM attempts WHERE agent_id=? ORDER BY rowid DESC LIMIT 1",
                [self.section["agent_id"].as_str().unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        drop(db);
        let catalog = self.config.resolve_catalog(accounts).unwrap();
        agent_run_adapters::provider::materialize_selected(
            &self.config,
            &catalog,
            &self.identity.authority.provider,
            &self.identity.authority.model,
            &account.parse().unwrap(),
            &self.role,
            work,
            home,
            app_home,
        )
        .unwrap();
    }

    /// Persist the probe's raw captures and verdicts before any assertion.
    pub fn write_evidence(&self, evidence: &Value) {
        std::fs::write(
            self.path("evidence"),
            serde_json::to_vec_pretty(evidence).unwrap(),
        )
        .unwrap();
    }
}

/// Loads `harness`'s section, or `None` when no live probe is requested.
pub(crate) fn load(harness: &str) -> Option<LiveManaged> {
    let spec = std::env::var("AGENT_RUN_LIVE_MANAGED_PROBE").ok()?;
    let spec: Value = serde_json::from_str(&std::fs::read_to_string(spec).unwrap()).unwrap();
    let home = PathBuf::from(spec["run_home"].as_str().unwrap());
    let section = spec[harness].clone();
    let db = rusqlite::Connection::open_with_flags(
        home.join("state.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    let (identity, request): (String, String) = db
        .query_row(
            "SELECT identity_json,request_json FROM agents WHERE id=?",
            [section["agent_id"].as_str().unwrap()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    drop(db);
    let identity: crate::service::ProviderLaunchIdentity = serde_json::from_str(&identity).unwrap();
    let request: crate::domain::StartRequest = serde_json::from_str(&request).unwrap();
    let role = ResolvedRolePlan::from_payload(&identity.authority.role_payload).unwrap();
    // As the supervisor does for every new launch: frozen role semantics,
    // current component build pins.
    let current = ProviderConfig::load(&home).unwrap().0;
    let config = agent_run_adapters::managed_role::launch_build_pins(
        &identity.provider_config,
        &current,
        identity.authority.harness,
    )
    .unwrap();
    let selection = agent_run_adapters::managed_role::select(
        &config,
        identity.authority.harness,
        &role,
        request.explicit_finish,
    )
    .unwrap()
    .expect("the probed attempt is a managed role");
    Some(LiveManaged {
        run_home: home,
        section,
        identity,
        request,
        role,
        config,
        selection,
    })
}
