//! Private home initialization compatible with the Python command.

use crate::{config::Config, fs, state::Store, Result};
use agent_run_domain::error::invalid;
use serde_json::{json, Value};
use std::path::Path;

/// Creates or validates the minimal private home without provisioning credentials.
///
/// Existing valid files remain untouched.  A missing `config.toml` is seeded
/// as an explicitly empty schema-2 catalog (no harness, provider or account;
/// nothing startable until the operator declares them) with empty default
/// role and skill directories, then configuration
/// and the SQLite schema are validated. A schema-1 config never seeds a new
/// state database: that would pair it with the current schema, which only
/// `config migrate` may do. Symlinked config files and non-directory homes
/// are refused before state is initialized.
pub fn initialize(home: &Path) -> Result<Value> {
    if let Ok(metadata) = std::fs::symlink_metadata(home) {
        if !metadata.is_dir() {
            return Err(invalid("agent-run home must be a directory"));
        }
    }
    fs::private_dir(home)?;
    let config = home.join("config.toml");
    if std::fs::symlink_metadata(&config).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(invalid("config.toml must not be a symlink"));
    }
    let dir = fs::Dir::open(home)?;
    if dir
        .optional(Path::new("config.toml"), 1024 * 1024)?
        .is_none()
    {
        dir.write(Path::new("config.toml"), b"schema_version = 2\n", 0o600)?;
        // The seeded config uses the default role and skill catalogs; they
        // start empty.
        fs::private_dir(&home.join("profiles"))?;
        fs::private_dir(&home.join("skills"))?;
    }
    // A valid schema-2 provider config is accepted as is; a schema-1 config
    // is validated only for an existing (already paired) state database.
    if agent_run_config::provider_config::ProviderConfig::load(home).is_err() {
        let _ = Config::load(home)?;
        if !home.join("state.db").exists() {
            return Err(invalid(
                "a schema_version 1 config cannot initialize new state; write a schema_version 2 config",
            ));
        }
    }
    let store = Store::initialize(home)?;
    drop(store);
    Ok(json!({"home": path(home), "config": path(&config), "state": path(&home.join("state.db"))}))
}

/// Serializes an installation path consistently with Python's CLI JSON encoder.
fn path(value: &Path) -> String {
    value.display().to_string()
}
