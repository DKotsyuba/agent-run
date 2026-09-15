#![allow(dead_code)]
use agent_run::{config::Config, domain::StartRequest, fs, state::Store};
use serde_json::json;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

pub struct Home { pub temp: TempDir, pub path: PathBuf, pub config: Config }
impl Home {
    pub fn new() -> Self {
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().canonicalize().unwrap();
        fs::private_dir(&path).unwrap();
        let binary = if Path::new("/usr/bin/true").is_file() { "/usr/bin/true" } else { "/bin/true" };
        let text = format!(
            "schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary={}\nhome={}\nmodels=['fixture']\nlimits_source='none'\n",
            toml::Value::String(binary.into()), toml::Value::String(path.join("runtimes/mock").to_string_lossy().into_owned())
        );
        std::fs::write(path.join("config.toml"), text).unwrap();
        let config = Config::load(&path).unwrap();
        Store::initialize(&path).unwrap();
        Self { temp, path, config }
    }
    pub fn request(&self) -> StartRequest {
        let mut r: StartRequest = serde_json::from_value(json!({
            "runtime":"mock", "model":"fixture", "profile":"review", "task":"fixture task", "workdir":self.path
        })).unwrap();
        r.validate().unwrap(); r
    }
    pub fn store(&self) -> Store { Store::open(&self.path).unwrap() }
}
