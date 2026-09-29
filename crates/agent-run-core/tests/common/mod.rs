#![allow(dead_code)]
use agent_run_config::config::Config;
use agent_run_domain::domain::StartRequest;
use agent_run_platform::fs;
use agent_run_store::Store;
use serde_json::json;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// One valid read-only resolved-role payload with its self-consistent
/// canonical config revision.
pub fn role_payload() -> serde_json::Value {
    let mut payload = json!({
        "role_name": "review",
        "role_revision": "fixture",
        "prompt": "Review the fixture.",
        "grants": {
            "write": false,
            "network": false,
            "allow_external_read_roots": false,
            "read_roots": [],
        },
        "skills": [],
        "mcp": [],
        "required_constraints": [],
        "auth": {"mode": "global", "reference": null},
    });
    let revision = agent_run_domain::canonical::sha256_hex(&payload, true);
    payload["config_revision"] = json!(revision);
    payload
}

/// One minimal frozen launch plan whose only qualification inputs are the
/// harness binary and its environment, for driving the real guard
/// qualification body with an isolated fixture harness.
pub fn launch_plan(binary: &Path) -> agent_run_adapters::provider::ProviderLaunchPlan {
    let role = agent_run_config::role_plan::ResolvedRolePlan::from_payload(&role_payload())
        .expect("role payload");
    agent_run_adapters::provider::ProviderLaunchPlan {
        launch: agent_run_adapters::LaunchPlan {
            binary: binary.to_path_buf(),
            args: vec!["app-server".into()],
            cwd: std::env::temp_dir(),
            environment: Default::default(),
            initial_input: None,
        },
        native_model: "fixture".into(),
        role,
        runtime: serde_json::from_value(json!({
            "enabled": true, "adapter": "codex", "binary": "/usr/bin/true",
            "home": "/tmp/agent-run-fixture-runtime",
            "models": ["fixture"],
        }))
        .expect("runtime"),
        profile: agent_run_config::profiles::Profile {
            name: "review".into(),
            body: "Review the fixture.".into(),
            write: false,
            network: false,
            revision: "fixture".into(),
            canonical: false,
            allow_external_read_roots: true,
            read_roots: vec![],
            skills: vec![],
            mcp: vec![],
            mcp_tools: Default::default(),
            required_constraints: Default::default(),
        },
    }
}

pub struct Home {
    pub temp: TempDir,
    pub path: PathBuf,
    pub config: Config,
}
impl Home {
    pub fn new() -> Self {
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().canonicalize().unwrap();
        fs::private_dir(&path).unwrap();
        let binary = if Path::new("/usr/bin/true").is_file() {
            "/usr/bin/true"
        } else {
            "/bin/true"
        };
        let text = format!("schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary={}\nhome={}\nmodels=['fixture']\nlimits_source='none'\n", toml::Value::String(binary.into()), toml::Value::String(path.join("runtimes/mock").to_string_lossy().into_owned()));
        std::fs::write(path.join("config.toml"), text).unwrap();
        let config = Config::load(&path).unwrap();
        Store::initialize(&path).unwrap();
        Self { temp, path, config }
    }
    pub fn request(&self) -> StartRequest {
        let mut request: StartRequest = serde_json::from_value(json!({"runtime":"mock", "model":"fixture", "profile":"review", "task":"fixture task", "workdir":self.path})).unwrap();
        request.validate().unwrap();
        request
    }
    pub fn store(&self) -> Store {
        Store::open(&self.path).unwrap()
    }
}
