//! The one-time paired migration: schema-1 config + older state database →
//! schema-2 config + current state database, and its paired rollback.
//!
//! * Planning (`--dry-run`), every refusal and every invalid input never open
//!   the state database for writing: no numbered migration, no account
//!   registration, no config change. The database is only read through its
//!   file header or a read-only connection.
//! * `--apply` holds the broker startup lock (`.api.sock.lock`) for its whole
//!   duration: a running broker makes it refuse, and a broker cannot start
//!   while it runs. It refuses active agents, then writes an exclusive,
//!   read-only snapshot (original config bytes, an online SQLite backup taken
//!   through a read-only connection, the mapping, `manifest.json` with the old
//!   and new binary identities and digests, `COMPLETE` last). Only then does
//!   it run the numbered store migration, register the mapping's declared
//!   accounts and publish the v2 config — after re-checking the config bytes
//!   it planned from. Any failure after the database was touched restores the
//!   snapshot database and the original config. Success records
//!   `<snapshot>.applied.json` with the published config digest and a logical
//!   digest of every database row.
//! * `rollback` restores the verified config/database pair only while both
//!   still equal the applied record (any later row change — an event, an
//!   account, a quota sample, an attempt update — is a divergence) and the
//!   recorded old binary still has its recorded digest; the operator points
//!   the installation back at that binary.
//!
//! Limits: the pair is restored as a whole or not at all; work done after the
//! migration cannot be preserved by rollback, which refuses instead.

use crate::{config::Config, fs, state::Store, Result};
use agent_run_config::{
    provider_config::ProviderConfig,
    provider_migration::{plan_v1, MigrationMapping},
};
use agent_run_domain::error::invalid;
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

/// The state database file of `home`.
fn db_path(home: &Path) -> PathBuf {
    home.join("state.db")
}

/// Reads the SQLite `user_version` without opening a connection: the file
/// header when no WAL frames exist, else a read-only connection (which may
/// only touch the shared-memory index). `None` when there is no database.
pub fn stored_version(home: &Path) -> Result<Option<i64>> {
    let path = db_path(home);
    let Ok(mut file) = std::fs::File::open(&path) else {
        return Ok(None);
    };
    let wal = home.join("state.db-wal");
    if wal.metadata().is_ok_and(|meta| meta.len() > 0) {
        let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        return Ok(Some(conn.pragma_query_value(
            None,
            "user_version",
            |row| row.get(0),
        )?));
    }
    let mut header = [0u8; 64];
    file.read_exact(&mut header)
        .map_err(|_| invalid("state database header is unreadable"))?;
    Ok(Some(
        u32::from_be_bytes([header[60], header[61], header[62], header[63]]) as i64,
    ))
}

/// Refuses every ordinary command while the home still has a state database
/// older than this binary's schema: opening it would run the numbered
/// migration unpaired with the config. `config migrate` is the only way on.
pub fn require_current_store(home: &Path) -> Result<()> {
    match stored_version(home)? {
        Some(version) if version < agent_run_store::VERSION => Err(invalid(format!(
            "migration_required: state database is schema v{version}; run `agent-run config migrate` before any other command"
        ))),
        _ => Ok(()),
    }
}

/// Takes the broker startup lock exclusively, refusing while a broker holds
/// it. Holding the returned file keeps any broker from starting.
fn exclusive(home: &Path) -> Result<std::fs::File> {
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(home.join(".api.sock.lock"))?;
    lock.try_lock_exclusive()
        .map_err(|_| invalid("stop the resident broker before a config migration"))?;
    Ok(lock)
}

/// Opens the state database strictly read-only. Without live WAL frames the
/// main file is complete, so it is opened `immutable` and SQLite creates no
/// `-wal`/`-shm` side files; otherwise the existing side files are read.
fn read_only(home: &Path) -> Result<Connection> {
    let path = db_path(home);
    if home.join("state.db-wal").exists() {
        return Ok(Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?);
    }
    Ok(Connection::open_with_flags(
        format!("file:{}?immutable=1", path.display()),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?)
}

/// Counts active agents through `conn` (any supported schema).
fn active_agents(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM agents WHERE status IN {}",
            agent_run_store::ACTIVE_SQL
        ),
        [],
        |row| row.get(0),
    )?)
}

/// A logical digest of every row of every table, independent of page layout
/// and WAL state; any post-migration write changes it.
fn content_digest(conn: &Connection) -> Result<String> {
    let mut tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    tables.sort();
    let mut text = String::new();
    for table in tables {
        let mut statement = conn.prepare(&format!("SELECT * FROM \"{table}\""))?;
        let columns = statement.column_count();
        let mut rows: Vec<String> = Vec::new();
        let mut query = statement.query([])?;
        while let Some(row) = query.next()? {
            let mut line = String::new();
            for index in 0..columns {
                line.push_str(&format!(
                    "{:?}\u{1f}",
                    row.get::<_, rusqlite::types::Value>(index)?
                ));
            }
            rows.push(line);
        }
        rows.sort();
        text.push_str(&format!("{table}\u{1e}{}\u{1d}", rows.join("\u{1e}")));
    }
    Ok(fs::sha256(text.as_bytes()))
}

/// Records one binary's path, SHA-256 and `--version` output.
fn binary_identity(path: &Path) -> Result<Value> {
    let bytes = std::fs::read(path).map_err(|_| invalid("binary is unreadable"))?;
    let version = std::process::Command::new(path)
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned());
    Ok(json!({"path": path, "sha256": fs::sha256(&bytes), "version": version}))
}

/// Plans from exact config and mapping bytes; reads no database.
fn plan(
    home: &Path,
    config: &[u8],
    mapping: &[u8],
) -> Result<(String, Value, Vec<String>, MigrationMapping)> {
    let text = std::str::from_utf8(config).map_err(|_| invalid("config must be UTF-8"))?;
    let old: Config = toml::from_str(text)
        .map_err(|_| invalid("config migrate expects a valid schema_version 1 config"))?;
    if old.schema_version != 1 {
        return Err(invalid("config migrate expects a schema_version 1 home"));
    }
    let mapping: MigrationMapping =
        toml::from_str(std::str::from_utf8(mapping).map_err(|_| invalid("mapping must be UTF-8"))?)
            .map_err(|_| invalid("invalid migration mapping shape or field"))?;
    let plan = plan_v1(
        &old,
        mapping.harnesses.clone(),
        mapping.runtimes.clone(),
        mapping.account_records(),
        home,
    )?;
    let rendered = format!(
        "{}\n",
        toml::to_string(&plan.config).map_err(|_| invalid("cannot render v2 config"))?
    );
    ProviderConfig::parse(&rendered, home)?;
    Ok((
        rendered,
        serde_json::to_value(&plan.legacy_runtime_map)?,
        plan.manual_review,
        mapping,
    ))
}

/// Creates `path` exclusively (never replacing anything) with `bytes`,
/// synced and read-only.
fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o400)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

/// Atomically replaces the live database with `source`'s bytes: a private
/// temporary copy is synced, stale WAL/shm files are removed, then renamed.
fn restore_db(home: &Path, source: &Path) -> Result<()> {
    let temporary = home.join(format!(".state.db.restore-{}", uuid::Uuid::new_v4()));
    std::fs::copy(source, &temporary)?;
    std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
    std::fs::File::open(&temporary)?.sync_all()?;
    for side in ["state.db-wal", "state.db-shm"] {
        match std::fs::remove_file(home.join(side)) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
            _ => {}
        }
    }
    std::fs::rename(&temporary, db_path(home))?;
    Ok(())
}

/// `config migrate --mapping FILE (--dry-run | --apply --from-binary OLD) [--ack M]...`.
///
/// Dry run returns the rendered v2 config, its digest, the historical
/// runtime map, manual-review markers and the database's current schema
/// version, and writes nothing. Apply is the paired operation described in
/// the module documentation; `from_binary` is the currently installed
/// (old) agent-run executable the rollback must return to.
pub fn migrate(
    home: &Path,
    mapping_path: &Path,
    apply: bool,
    acks: &[String],
    from_binary: Option<&Path>,
) -> Result<Value> {
    let config_bytes = std::fs::read(home.join("config.toml"))?;
    let mapping_bytes = std::fs::read(mapping_path)?;
    let (rendered, legacy_runtime_map, review, mapping) =
        plan(home, &config_bytes, &mapping_bytes)?;
    let summary = json!({
        "config_toml": rendered,
        "config_sha256": fs::sha256(rendered.as_bytes()),
        "legacy_runtime_map": legacy_runtime_map,
        "manual_review": review,
        "state_schema_version": stored_version(home)?,
        "target_schema_version": agent_run_store::VERSION,
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
    let from_binary = from_binary
        .ok_or_else(|| invalid("--apply requires --from-binary <installed agent-run>"))?;
    let old_binary = binary_identity(from_binary)?;
    let target_binary = binary_identity(&std::env::current_exe()?)?;
    if old_binary["sha256"] == target_binary["sha256"] {
        return Err(invalid(
            "--from-binary must be the installed pre-migration agent-run, not this one",
        ));
    }
    if !db_path(home).is_file() {
        return Err(invalid("config migrate expects an existing state database"));
    }
    let _lock = exclusive(home)?;
    // Everything below runs with brokers excluded. Refusals write nothing.
    let source = read_only(home)?;
    if active_agents(&source)? > 0 {
        return Err(invalid("active agents must finish before a config switch"));
    }
    if std::fs::read(home.join("config.toml"))? != config_bytes {
        return Err(invalid(
            "config.toml changed while planning; rerun the dry run",
        ));
    }
    let root = home.join("migrations");
    fs::private_dir(&root)?;
    let at = crate::domain::now();
    let dir = root.join(format!(
        "{}-{}-v1-to-v2",
        at as u64,
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::DirBuilder::new().mode(0o700).create(&dir)?;
    write_new(&dir.join("config.v1.toml"), &config_bytes)?;
    write_new(&dir.join("mapping.toml"), &mapping_bytes)?;
    source.backup(rusqlite::DatabaseName::Main, dir.join("state.db"), None)?;
    std::fs::set_permissions(dir.join("state.db"), std::fs::Permissions::from_mode(0o400))?;
    let backup = Connection::open_with_flags(
        format!("file:{}?immutable=1", dir.join("state.db").display()),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    let source_version: i64 = backup.pragma_query_value(None, "user_version", |row| row.get(0))?;
    let source_digest = content_digest(&backup)?;
    drop(backup);
    drop(source);
    let manifest = json!({
        "kind": "agent-run-paired-migration",
        "created_at": at,
        "old_binary": old_binary,
        "target_binary": target_binary,
        "v1_config_sha256": fs::sha256(&config_bytes),
        "mapping_sha256": fs::sha256(&mapping_bytes),
        "v2_config_sha256": fs::sha256(rendered.as_bytes()),
        "source_schema_version": source_version,
        "target_schema_version": agent_run_store::VERSION,
        "state_backup_sha256": fs::sha256(&std::fs::read(dir.join("state.db"))?),
        "state_content_sha256": source_digest,
        "legacy_runtime_map": legacy_runtime_map,
        "manual_review": review,
    });
    write_new(&dir.join("manifest.json"), &fs::canonical_json(&manifest)?)?;
    write_new(&dir.join("COMPLETE"), b"")?;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500))?;
    // The pair changes from here; any failure restores the snapshot pair.
    match publish(home, &dir, &config_bytes, &rendered, &mapping) {
        Ok(applied) => {
            Ok(json!({"applied": true, "snapshot": dir, "record": applied, "plan": summary}))
        }
        Err(error) => {
            restore_db(home, &dir.join("state.db"))?;
            if std::fs::read(home.join("config.toml"))? != config_bytes {
                fs::Dir::open(home)?.write(Path::new("config.toml"), &config_bytes, 0o600)?;
            }
            Err(error)
        }
    }
}

/// Runs the numbered store migration, registers the declared accounts and
/// publishes the v2 config after re-checking the original config bytes, then
/// writes the applied record. Called only with brokers excluded.
fn publish(
    home: &Path,
    snapshot: &Path,
    config_bytes: &[u8],
    rendered: &str,
    mapping: &MigrationMapping,
) -> Result<Value> {
    let mut store = Store::open(home)?;
    for record in mapping.account_records() {
        store.register_account(&record)?;
    }
    drop(store);
    #[cfg(feature = "test-fixtures")]
    if std::env::var_os("AGENT_RUN_MIGRATE_FAULT").is_some() {
        return Err(invalid("injected migration publish fault"));
    }
    if std::fs::read(home.join("config.toml"))? != config_bytes {
        return Err(invalid(
            "config.toml changed during migration; nothing was published",
        ));
    }
    fs::Dir::open(home)?.write(Path::new("config.toml"), rendered.as_bytes(), 0o600)?;
    let record = json!({
        "v2_config_sha256": fs::sha256(rendered.as_bytes()),
        "state_content_sha256": content_digest(&read_only(home)?)?,
        "applied_at": crate::domain::now(),
    });
    write_new(
        &snapshot.with_extension("applied.json"),
        &fs::canonical_json(&record)?,
    )?;
    Ok(record)
}

/// `config rollback --snapshot DIR`: restores the verified original pair.
///
/// Refuses unless the snapshot is complete and matches its manifest, the
/// applied record exists, the live config and every database row still equal
/// what the migration published, no broker or agent is live, and the recorded
/// old binary still has its recorded digest. Restores the config and then the
/// database; the operator then runs the recorded old binary.
pub fn rollback(home: &Path, snapshot: &Path) -> Result<Value> {
    let dir: PathBuf = snapshot.to_path_buf();
    if !dir.join("COMPLETE").is_file() {
        return Err(invalid("migration snapshot is incomplete"));
    }
    let manifest: Value = serde_json::from_slice(&std::fs::read(dir.join("manifest.json"))?)?;
    let applied: Value = serde_json::from_slice(
        &std::fs::read(dir.with_extension("applied.json"))
            .map_err(|_| invalid("migration was never applied from this snapshot"))?,
    )?;
    let v1 = std::fs::read(dir.join("config.v1.toml"))?;
    let expect = |value: &Value, key: &str| value[key].as_str().unwrap_or_default().to_owned();
    if fs::sha256(&v1) != expect(&manifest, "v1_config_sha256")
        || fs::sha256(&std::fs::read(dir.join("state.db"))?)
            != expect(&manifest, "state_backup_sha256")
    {
        return Err(invalid("migration snapshot does not match its manifest"));
    }
    let old = &manifest["old_binary"];
    let old_path = PathBuf::from(old["path"].as_str().unwrap_or_default());
    if std::fs::read(&old_path)
        .map(|bytes| fs::sha256(&bytes))
        .ok()
        .as_deref()
        != old["sha256"].as_str()
    {
        return Err(invalid(
            "the recorded pre-migration binary is missing or changed; restore it first",
        ));
    }
    let _lock = exclusive(home)?;
    let current = fs::sha256(&std::fs::read(home.join("config.toml"))?);
    let resuming = current == expect(&manifest, "v1_config_sha256");
    if !resuming && current != expect(&applied, "v2_config_sha256") {
        return Err(invalid(
            "config.toml changed after migration; rollback would discard it",
        ));
    }
    if !resuming {
        let live = read_only(home)?;
        if active_agents(&live)? > 0 {
            return Err(invalid("active agents must finish before rollback"));
        }
        if content_digest(&live)? != expect(&applied, "state_content_sha256") {
            return Err(invalid(
                "state changed after migration (rows written); rollback cannot preserve them",
            ));
        }
        drop(live);
        fs::Dir::open(home)?.write(Path::new("config.toml"), &v1, 0o600)?;
    }
    // Config is schema 1 now; a failed database restore leaves a newer
    // database that the old binary refuses, and this command can be rerun.
    restore_db(home, &dir.join("state.db"))?;
    if content_digest(&read_only(home)?)? != expect(&manifest, "state_content_sha256") {
        return Err(invalid("restored database does not match the snapshot"));
    }
    Ok(json!({
        "rolled_back": true,
        "config_sha256": fs::sha256(&v1),
        "state_schema_version": manifest["source_schema_version"],
        "run_binary": old,
    }))
}
