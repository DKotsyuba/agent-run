//! One-time schema-1 → schema-2 configuration migration and its rollback.
//!
//! The only durable mutation is one atomic replacement of `config.toml`; the
//! SQLite state is never modified here (schema-17 store migrations already
//! run on open, and accounts are registered beforehand with `accounts
//! register`). `--apply` first writes a recoverable snapshot directory
//! `<home>/migrations/<unix-seconds>-v1-to-v2/` holding the exact v1 config
//! bytes, an online SQLite backup (safe under WAL; the live main file is
//! never copied alone), and `manifest.json` recording the binary path,
//! version and SHA-256, the v1/v2 config digests, the backup digest, and the
//! agent count; `COMPLETE` is written last. Rollback restores the v1 config
//! only after verifying that snapshot and that nothing diverged since.

use crate::{config::Config, fs, state::Store, Result};
use agent_run_config::{
    provider_config::ProviderConfig,
    provider_migration::{plan_v1, MigrationMapping},
};
use agent_run_domain::error::invalid;
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

/// Refuses while any agent is active or a resident broker answers on
/// `api.sock`: either could race the configuration switch. Nothing is killed.
fn refuse_live(home: &Path, store: &Store) -> Result<()> {
    let active: i64 = store.conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM agents WHERE status IN {}",
            agent_run_store::ACTIVE_SQL
        ),
        [],
        |row| row.get(0),
    )?;
    if active > 0 {
        return Err(invalid("active agents must finish before a config switch"));
    }
    if std::os::unix::net::UnixStream::connect(home.join("api.sock")).is_ok() {
        return Err(invalid("stop the resident broker before a config switch"));
    }
    Ok(())
}

/// Counts every durable agent row; rollback requires this unchanged.
fn agent_count(store: &Store) -> Result<i64> {
    Ok(store
        .conn
        .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))?)
}

/// Plans the migration of `home`'s schema-1 config with the TOML mapping at
/// `mapping`, returning the rendered and re-validated v2 text and the plan's
/// manual-review markers. Reads only config, the mapping, and the account
/// registry; never a credential value.
fn plan(home: &Path, mapping: &Path) -> Result<(String, Value, Vec<String>)> {
    let old = Config::load(home)?;
    if old.schema_version != 1 {
        return Err(invalid("config migrate expects a schema_version 1 home"));
    }
    let text = std::fs::read_to_string(mapping)?;
    let mapping: MigrationMapping =
        toml::from_str(&text).map_err(|_| invalid("invalid migration mapping shape or field"))?;
    let accounts = Store::open(home)?.list_accounts()?;
    let plan = plan_v1(&old, mapping.harnesses, mapping.runtimes, accounts, home)?;
    let rendered = format!(
        "{}\n",
        toml::to_string(&plan.config).map_err(|_| invalid("cannot render v2 config"))?
    );
    // The emitted file must parse exactly as a normal v2 load would.
    ProviderConfig::parse(&rendered, home)?;
    Ok((
        rendered,
        serde_json::to_value(&plan.legacy_runtime_map)?,
        plan.manual_review,
    ))
}

/// `config migrate --mapping FILE (--dry-run | --apply) [--ack MARKER]...`.
///
/// Dry run returns the rendered v2 config, its digest, the historical
/// runtime map and manual-review markers, writing nothing. Apply
/// additionally requires every manual-review marker to be acknowledged
/// exactly, no active agent or broker, then writes the snapshot and
/// atomically publishes the v2 config. Any failure before that final rename
/// leaves the original config untouched.
pub fn migrate(home: &Path, mapping: &Path, apply: bool, acks: &[String]) -> Result<Value> {
    let (rendered, legacy_runtime_map, review) = plan(home, mapping)?;
    let v2_sha = fs::sha256(rendered.as_bytes());
    let summary = json!({
        "config_toml": rendered,
        "config_sha256": v2_sha,
        "legacy_runtime_map": legacy_runtime_map,
        "manual_review": review,
    });
    if !apply {
        return Ok(json!({"applied": false, "plan": summary}));
    }
    let acknowledged: BTreeSet<&str> = acks.iter().map(String::as_str).collect();
    if acknowledged != review.iter().map(String::as_str).collect() {
        return Err(invalid(
            "--ack must name exactly every manual_review marker of the dry run",
        ));
    }
    let store = Store::open(home)?;
    refuse_live(home, &store)?;
    let v1_bytes = std::fs::read(home.join("config.toml"))?;
    let at = crate::domain::now();
    let dir = home
        .join("migrations")
        .join(format!("{}-v1-to-v2", at as u64));
    fs::private_dir(&home.join("migrations"))?;
    fs::private_dir(&dir)?;
    let snapshot = fs::Dir::open(&dir)?;
    snapshot.write(Path::new("config.v1.toml"), &v1_bytes, 0o600)?;
    store.backup(&dir.join("state.db"))?;
    let exe = std::env::current_exe()?;
    let manifest = json!({
        "kind": "agent-run-config-migration",
        "created_at": at,
        "binary": {"path": exe, "version": env!("CARGO_PKG_VERSION"),
                   "sha256": fs::sha256(&std::fs::read(&exe)?)},
        "v1_config_sha256": fs::sha256(&v1_bytes),
        "v2_config_sha256": v2_sha,
        "state_backup_sha256": fs::sha256(&std::fs::read(dir.join("state.db"))?),
        "agents": agent_count(&store)?,
        "legacy_runtime_map": legacy_runtime_map,
        "manual_review": review,
        "rollback": "agent-run config rollback --snapshot <this directory>: restores config.v1.toml only while config.toml still has v2_config_sha256 and the agent count is unchanged; state.db here is a disaster-recovery copy restored only manually with everything stopped",
    });
    snapshot.write(
        Path::new("manifest.json"),
        &fs::canonical_json(&manifest)?,
        0o600,
    )?;
    snapshot.write(Path::new("COMPLETE"), b"", 0o600)?;
    drop(store);
    fs::Dir::open(home)?.write(Path::new("config.toml"), rendered.as_bytes(), 0o600)?;
    Ok(json!({"applied": true, "snapshot": dir, "plan": summary}))
}

/// `config rollback --snapshot DIR`: restores the snapshot's v1 config.
///
/// Refuses unless `COMPLETE` exists, the snapshot files match the manifest
/// digests, the current `config.toml` is still exactly the migrated v2 file,
/// the agent count is unchanged (so no post-migration run would be orphaned
/// under a v1 config), and no agent or broker is live. Only `config.toml`
/// changes; the SQLite state is left as is.
pub fn rollback(home: &Path, snapshot: &Path) -> Result<Value> {
    let dir: PathBuf = snapshot.to_path_buf();
    if !dir.join("COMPLETE").is_file() {
        return Err(invalid("migration snapshot is incomplete"));
    }
    let manifest: Value = serde_json::from_slice(&std::fs::read(dir.join("manifest.json"))?)?;
    let v1 = std::fs::read(dir.join("config.v1.toml"))?;
    let digest = |value: &str| manifest[value].as_str().unwrap_or_default().to_owned();
    if fs::sha256(&v1) != digest("v1_config_sha256")
        || fs::sha256(&std::fs::read(dir.join("state.db"))?) != digest("state_backup_sha256")
    {
        return Err(invalid("migration snapshot does not match its manifest"));
    }
    if fs::sha256(&std::fs::read(home.join("config.toml"))?) != digest("v2_config_sha256") {
        return Err(invalid(
            "config.toml changed after migration; rollback would discard that edit",
        ));
    }
    let store = Store::open(home)?;
    refuse_live(home, &store)?;
    if Some(agent_count(&store)?) != manifest["agents"].as_i64() {
        return Err(invalid(
            "agents were created after migration; rollback cannot preserve them",
        ));
    }
    drop(store);
    fs::Dir::open(home)?.write(Path::new("config.toml"), &v1, 0o600)?;
    Config::load(home)?;
    Ok(json!({"rolled_back": true, "config_sha256": fs::sha256(&v1)}))
}
