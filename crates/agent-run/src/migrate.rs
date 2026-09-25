//! Paired migration from a legacy mapping or an explicit replacement v2 config
//! to the current state database, with byte-exact paired rollback.
//!
//! * Planning (`--dry-run`), every refusal and every invalid input never open
//!   the state database for writing: no numbered migration, no account
//!   registration, no config change. The database is only read through its
//!   file header or a read-only connection.
//! * `--apply` names the installed pre-migration release (`--from-release`), a
//!   sealed release directory whose `COMPLETE`/`SHA256SUMS`/`metadata.json`
//!   must verify and whose recorded schema must equal the database's; nothing
//!   is executed to learn its version. It holds the broker startup lock
//!   (`.api.sock.lock`) throughout, refuses active agents, and writes an
//!   exclusive, read-only snapshot (original config bytes, an online SQLite
//!   backup, the mapping, `manifest.json`, `COMPLETE` last). The numbered
//!   migration and the declared accounts are applied to a staged copy of the
//!   snapshot database, never to the live file.
//! * Publication is journalled: `migrations/in-progress.json` records the
//!   snapshot and the exact source and target row digests and config digests
//!   before anything live changes. The live database is then replaced from the
//!   staged target in one SQLite backup transaction, the v2 config is written
//!   (only while the config still has the exact bytes planned from), then
//!   `<snapshot>.applied.json`, and the journal is removed. While a journal
//!   exists every ordinary command and the broker refuse (`migration_incomplete`).
//! * Failure recovery and `rollback` touch only state they can prove: the
//!   config only while it equals the v1 or v2 bytes of this snapshot, the
//!   database only while every row equals the recorded source or target
//!   digest. Anything else — a third-party config edit, a later row write —
//!   is left exactly as found and refused.
//!
//! Writer exclusion: besides the broker lock and the `migration_required` /
//! `migration_incomplete` gate for newly started commands, apply and rollback
//! hold one exclusive SQLite lease on the live database ([`lease`]) from
//! before the first live read until the journal is cleared. Every live read
//! (active agents, row digest, the snapshot backup) and every replacement run
//! through that same connection. A process that already holds the database
//! open makes acquisition refuse before anything is written; a writer that
//! arrives later is refused `SQLITE_BUSY` until the lease is released, so no
//! committed write is ever replaced.
//!
//! Historical snapshot filenames and v1/v2 digest keys mean before/after for
//! both input schemas. Recovery restores the original pair; there is no roll-forward.

use crate::{config::Config, fs, state::Store, Result};
use agent_run_config::{
    provider_config::ProviderConfig,
    provider_migration::{plan_v1, MigrationMapping},
};
use agent_run_domain::error::invalid;
use agent_run_platform::release;
use fs2::FileExt;
use rusqlite::{functions::FunctionFlags, Connection, OpenFlags};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
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

/// The durable record of an apply or rollback in progress.
fn journal_path(home: &Path) -> PathBuf {
    home.join("migrations").join("in-progress.json")
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

/// Refuses every ordinary command while a migration or rollback is
/// unfinished, or while the home still has a state database older than this
/// binary's schema: opening it would run the numbered migration unpaired with
/// the config. `config migrate` / `config rollback` are the only ways on.
pub fn require_current_store(home: &Path) -> Result<()> {
    if journal_path(home).exists() {
        let snapshot = std::fs::read(journal_path(home))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .and_then(|journal| journal["snapshot"].as_str().map(str::to_owned))
            .unwrap_or_else(|| "<snapshot>".into());
        return Err(invalid(format!(
            "migration_incomplete: an interrupted config migration or rollback holds this home; run `agent-run config rollback --snapshot {snapshot}`"
        )));
    }
    match stored_version(home)? {
        Some(version) if version < agent_run_store::VERSION => Err(invalid(format!(
            "migration_required: state database is schema v{version}; run `agent-run config migrate` before any other command"
        ))),
        _ => Ok(()),
    }
}

/// Holds both broker and service-manager startup locks, including custom-socket brokers.
/// Returned files retain exclusion throughout publication and rollback.
fn exclusive(home: &Path) -> Result<(std::fs::File, std::fs::File)> {
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(home.join(".api.sock.lock"))?;
    lock.try_lock_exclusive()
        .map_err(|_| invalid("stop the resident broker before a config migration"))?;
    let services = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(home.join(".services.lock"))?;
    services.try_lock_exclusive().map_err(|_| {
        invalid("stop the resident broker and its service manager before migration")
    })?;
    Ok((lock, services))
}

/// Reads one operator-supplied configuration or mapping with the same one-MiB bound as runtime configuration.
fn input_bytes(path: &Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1024 * 1024 {
        return Err(invalid("migration configuration exceeds one MiB"));
    }
    Ok(bytes)
}

/// Opens a state database strictly read-only. Without live WAL frames the
/// main file is complete, so it is opened `immutable` and SQLite creates no
/// `-wal`/`-shm` side files; otherwise the existing side files are read.
fn read_only_at(path: &Path) -> Result<Connection> {
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    if wal.exists() {
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

/// Hashes a file in fixed-size chunks, including multi-gigabyte database backups.
fn file_digest(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut bytes = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut bytes)?;
        if n == 0 {
            break;
        }
        hash.update(&bytes[..n]);
    }
    Ok(agent_run_domain::canonical::hex_digest(&hash.finalize()))
}

/// Quotes a schema-provided identifier, including embedded double quotes.
fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A logical digest of every row, byte-compatible with historical paired snapshots.
/// SQLite performs the binary row-text sort with temporary-file spill; Rust
/// holds only one row and the digest state, never the whole database in RAM.
fn content_digest(conn: &Connection) -> Result<String> {
    conn.pragma_update(None, "temp_store", "FILE")?;
    conn.create_scalar_function(
        "_agent_run_migration_row",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |context| {
            let mut line = String::new();
            for index in 0..context.len() {
                line.push_str(&format!(
                    "{:?}\u{1f}",
                    context.get::<rusqlite::types::Value>(index)?
                ));
            }
            Ok(line)
        },
    )?;
    let mut tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    tables.sort();
    let mut hash = Sha256::new();
    for table in tables {
        let columns = conn
            .prepare(&format!("SELECT * FROM {}", identifier(&table)))?
            .column_names()
            .into_iter()
            .map(identifier)
            .collect::<Vec<_>>()
            .join(",");
        let mut statement=conn.prepare(&format!("SELECT _agent_run_migration_row({columns}) AS payload FROM {} ORDER BY payload COLLATE BINARY",identifier(&table)))?;
        hash.update(table.as_bytes());
        hash.update(b"\x1e");
        let mut query = statement.query([])?;
        let mut first = true;
        while let Some(row) = query.next()? {
            if !first {
                hash.update(b"\x1e");
            }
            first = false;
            hash.update(row.get::<_, String>(0)?.as_bytes());
        }
        hash.update(b"\x1d");
    }
    Ok(agent_run_domain::canonical::hex_digest(&hash.finalize()))
}

/// Takes the exclusive lease on the live database: a read-write connection in
/// `locking_mode=EXCLUSIVE` that runs one empty `BEGIN EXCLUSIVE` and then
/// keeps the file lock, with no transaction open, until it is dropped. Any
/// connection that already has the database open or in use makes this
/// refuse (`SQLITE_BUSY`, no waiting); while it is held every other
/// connection's read or write is refused. The same connection must serve all
/// live reads and be the backup destination: closing it releases the lock.
fn lease(home: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(db_path(home), OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    conn.busy_timeout(std::time::Duration::ZERO)?;
    let mode: String = conn.query_row("PRAGMA locking_mode=EXCLUSIVE", [], |row| row.get(0))?;
    if mode != "exclusive" {
        return Err(invalid("state database refused exclusive locking mode"));
    }
    conn.execute_batch("BEGIN EXCLUSIVE; COMMIT;").map_err(|_| {
        invalid("state database is in use by another process; stop every agent-run process on this home first")
    })?;
    Ok(conn)
}

/// The live database's row digest and active-agent count, read through the
/// lease.
fn live_state(live: &Connection) -> Result<(String, i64)> {
    Ok((content_digest(live)?, active_agents(live)?))
}

/// Facts of the installed pre-migration release, read from its verified seal
/// (never by executing it): path, metadata version and supported schema, and
/// the digests of its binary and `SHA256SUMS`.
fn old_release(dir: &Path) -> Result<Value> {
    release::verify(dir)
        .map_err(|error| invalid(format!("--from-release is not a sealed release: {error}")))?;
    let metadata: Value = serde_json::from_slice(&std::fs::read(dir.join("metadata.json"))?)?;
    Ok(json!({
        "path": dir,
        "version": metadata["version"],
        "schema_version": release::schema_version(dir).map_err(invalid)?,
        "binary": dir.join("bin/agent-run"),
        "binary_sha256": file_digest(&dir.join("bin/agent-run"))?,
        "sha256sums_sha256": file_digest(&dir.join("SHA256SUMS"))?,
    }))
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

/// Durably records the operation in progress (temp write, rename, dir sync).
fn write_journal(home: &Path, journal: &Value) -> Result<()> {
    fs::Dir::open(&home.join("migrations"))?.write(
        Path::new("in-progress.json"),
        &fs::canonical_json(journal)?,
        0o600,
    )
}

/// Durably removes the journal once the pair is proven consistent again.
fn clear_journal(home: &Path) -> Result<()> {
    std::fs::remove_file(journal_path(home))?;
    std::fs::File::open(home.join("migrations"))?.sync_all()?;
    Ok(())
}

/// Replaces the live database's content with `source`'s in one SQLite backup
/// transaction (crash-safe through SQLite's own journal) whose destination is
/// the lease itself, then checkpoints so the main file carries the result.
/// The lease stays held throughout and afterwards.
fn replace_db(live: &mut Connection, source: &Path) -> Result<()> {
    let from = read_only_at(source)?;
    let done = rusqlite::backup::Backup::new(&from, live)?.step(-1)?;
    if !matches!(done, rusqlite::backup::StepResult::Done) {
        return Err(invalid("state database replacement did not complete"));
    }
    live.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
    Ok(())
}

/// Test-only fault and pause points between the publication steps, so a
/// test can return an error or hold (and kill) the real process there.
/// `AGENT_RUN_MIGRATE_PAUSE=<step>:<dir>` writes `<dir>/paused` and waits
/// for `<dir>/release`.
#[cfg(feature = "test-fixtures")]
fn checkpoint(step: &str) -> Result<()> {
    if std::env::var("AGENT_RUN_MIGRATE_FAULT").as_deref() == Ok(step) {
        return Err(invalid(format!("injected migration fault at {step}")));
    }
    if let Some((at, dir)) = std::env::var("AGENT_RUN_MIGRATE_PAUSE")
        .ok()
        .as_deref()
        .and_then(|value| value.split_once(':'))
        .map(|(at, dir)| (at.to_owned(), PathBuf::from(dir)))
    {
        if at == step {
            std::fs::write(dir.join("paused"), step)?;
            for _ in 0..600 {
                if dir.join("release").exists() {
                    return Ok(());
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            return Err(invalid("migration pause was never released"));
        }
    }
    Ok(())
}

/// Production builds have no fault or pause points.
#[cfg(not(feature = "test-fixtures"))]
fn checkpoint(_step: &str) -> Result<()> {
    Ok(())
}

/// `config migrate --mapping FILE (--dry-run | --apply --from-release DIR) [--ack M]...`.
///
/// Dry run returns the rendered v2 config, its digest, the historical
/// runtime map, manual-review markers and the database's current schema
/// version, and writes nothing. Apply is the paired operation described in
/// the module documentation; `from_release` is the installed (old) sealed
/// release the rollback returns to.
pub fn migrate(
    home: &Path,
    mapping_path: &Path,
    apply: bool,
    acks: &[String],
    from_release: Option<&Path>,
) -> Result<Value> {
    migrate_input(
        home,
        mapping_path,
        apply,
        acks,
        from_release,
        MigrationInput::LegacyMapping,
    )
}

/// Upgrades an already-v2 home using an explicit validated replacement config, preserving every account reference.
/// Dry run is read-only; apply and rollback reuse the same journalled paired executor as legacy migration.
pub fn migrate_v2(
    home: &Path,
    target_config: &Path,
    apply: bool,
    from_release: Option<&Path>,
) -> Result<Value> {
    migrate_input(
        home,
        target_config,
        apply,
        &[],
        from_release,
        MigrationInput::ProviderConfig,
    )
}

/// Selects how the operator describes the target, without changing publication or recovery semantics.
enum MigrationInput {
    /// Explicit legacy runtime-to-provider and account mapping.
    LegacyMapping,
    /// Complete v2 configuration, with no account registration or credential mutation.
    ProviderConfig,
}

/// Plans one bounded input, then performs the shared backup/stage/journal/publish sequence.
fn migrate_input(
    home: &Path,
    input_path: &Path,
    apply: bool,
    acks: &[String],
    from_release: Option<&Path>,
    input: MigrationInput,
) -> Result<Value> {
    let config_bytes = input_bytes(&home.join("config.toml"))?;
    let mapping_bytes = input_bytes(input_path)?;
    let (rendered, legacy_runtime_map, review, accounts, source_config_schema) = match input {
        MigrationInput::LegacyMapping => {
            let (rendered, map, review, mapping) = plan(home, &config_bytes, &mapping_bytes)?;
            (rendered, map, review, mapping.account_records(), 1)
        }
        MigrationInput::ProviderConfig => {
            let source: toml::Value = toml::from_str(
                std::str::from_utf8(&config_bytes).map_err(|_| invalid("config must be UTF-8"))?,
            )
            .map_err(|_| invalid("source configuration is not valid TOML"))?;
            if source
                .get("schema_version")
                .and_then(toml::Value::as_integer)
                != Some(2)
            {
                return Err(invalid(
                    "--target-config requires an already-v2 home; use --mapping for schema 1",
                ));
            }
            if stored_version(home)?.is_none_or(|version| version < 17) {
                return Err(invalid(
                    "a v2 home requires the existing schema-17 account registry",
                ));
            }
            let rendered = std::str::from_utf8(&mapping_bytes)
                .map_err(|_| invalid("target config must be UTF-8"))?
                .to_owned();
            let target = ProviderConfig::parse(&rendered, home)?;
            target.resolve_catalog(agent_run_store::accounts::list_at(&read_only_at(
                &db_path(home),
            )?)?)?;
            (rendered, json!({}), Vec::new(), Vec::new(), 2)
        }
    };
    let source_version = stored_version(home)?;
    let summary = json!({
        "config_toml": rendered,
        "config_sha256": fs::sha256(rendered.as_bytes()),
        "legacy_runtime_map": legacy_runtime_map,
        "manual_review": review,
        "state_schema_version": source_version,
        "target_schema_version": agent_run_store::VERSION,
        "source_config_schema": source_config_schema,
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
    let from_release = from_release
        .ok_or_else(|| invalid("--apply requires --from-release <installed sealed release>"))?;
    let source_version = source_version
        .ok_or_else(|| invalid("config migrate expects an existing state database"))?;
    let old = old_release(from_release)?;
    if old["schema_version"].as_i64() != Some(source_version) {
        return Err(invalid(format!(
            "--from-release supports schema {} but the state database is schema {source_version}",
            old["schema_version"]
        )));
    }
    if source_version >= agent_run_store::VERSION {
        return Err(invalid(format!(
            "state database schema {source_version} is not older than target schema {}",
            agent_run_store::VERSION
        )));
    }
    let target_exe = std::env::current_exe()?;
    let target = json!({
        "path": target_exe,
        "sha256": file_digest(&target_exe)?,
        "schema_version": agent_run_store::VERSION,
    });
    if old["binary_sha256"] == target["sha256"] {
        return Err(invalid(
            "--from-release must be the installed pre-migration release, not this binary",
        ));
    }
    let _lock = exclusive(home)?;
    // Everything below runs with brokers excluded. Refusals write nothing.
    require_no_journal(home)?;
    let mut live = lease(home)?;
    let source = &live;
    if active_agents(source)? > 0 {
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
    let name = format!(
        "{}-{}-{}",
        at as u64,
        uuid::Uuid::new_v4().simple(),
        if source_config_schema == 1 {
            "v1-to-v2"
        } else {
            "v2-state-upgrade"
        }
    );
    let dir = root.join(&name);
    std::fs::DirBuilder::new().mode(0o700).create(&dir)?;
    write_new(&dir.join("config.v1.toml"), &config_bytes)?;
    write_new(&dir.join("mapping.toml"), &mapping_bytes)?;
    source.backup(rusqlite::DatabaseName::Main, dir.join("state.db"), None)?;
    std::fs::set_permissions(dir.join("state.db"), std::fs::Permissions::from_mode(0o400))?;
    let backup = read_only_at(&dir.join("state.db"))?;
    let backup_version: i64 = backup.pragma_query_value(None, "user_version", |row| row.get(0))?;
    let source_digest = content_digest(&backup)?;
    drop(backup);
    if backup_version != source_version {
        return Err(invalid("state database changed schema while snapshotting"));
    }
    let manifest = json!({
        "kind": "agent-run-paired-migration",
        "created_at": at,
        "old_release": old,
        "target_binary": target,
        "v1_config_sha256": fs::sha256(&config_bytes),
        "mapping_sha256": fs::sha256(&mapping_bytes),
        "v2_config_sha256": fs::sha256(rendered.as_bytes()),
        "source_schema_version": source_version,
        "target_schema_version": agent_run_store::VERSION,
        "state_backup_sha256": file_digest(&dir.join("state.db"))?,
        "state_content_sha256": source_digest,
        "legacy_runtime_map": legacy_runtime_map,
        "manual_review": review,
        "source_config_schema": source_config_schema,
    });
    write_new(&dir.join("manifest.json"), &fs::canonical_json(&manifest)?)?;
    write_new(&dir.join("COMPLETE"), b"")?;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500))?;

    // The target is built on a staged copy; the live pair is still untouched.
    let stage = root.join(format!("{name}.stage"));
    std::fs::DirBuilder::new().mode(0o700).create(&stage)?;
    let target_digest = (|| {
        std::fs::copy(dir.join("state.db"), stage.join("state.db"))?;
        std::fs::set_permissions(
            stage.join("state.db"),
            std::fs::Permissions::from_mode(0o600),
        )?;
        let mut store = Store::open(&stage)?;
        for record in accounts {
            store.register_account(&record)?;
        }
        ProviderConfig::parse(&rendered, home)?.resolve_catalog(store.list_accounts()?)?;
        drop(store);
        checkpoint("staged")?;
        content_digest(&read_only_at(&stage.join("state.db"))?)
    })();
    let target_digest = match target_digest {
        Ok(digest) => digest,
        Err(error) => {
            let _ = std::fs::remove_dir_all(&stage);
            return Err(error);
        }
    };
    let journal = json!({
        "operation": "apply",
        "snapshot": dir,
        "source_state_sha256": source_digest,
        "target_state_sha256": target_digest,
        "v1_config_sha256": fs::sha256(&config_bytes),
        "v2_config_sha256": fs::sha256(rendered.as_bytes()),
    });
    // Nothing live has changed yet: a refusal here only drops the stage.
    let unchanged = (|| {
        if std::fs::read(home.join("config.toml"))? != config_bytes {
            return Err(invalid(
                "config.toml changed during migration; nothing was published",
            ));
        }
        if live_state(&live)?.0 != source_digest {
            return Err(invalid(
                "state changed during migration (a writer was still running); nothing was published",
            ));
        }
        Ok(())
    })();
    if let Err(error) = unchanged {
        let _ = std::fs::remove_dir_all(&stage);
        return Err(error);
    }
    write_journal(home, &journal)?;
    // The pair changes from here; a failure restores only what this
    // operation provably published.
    match publish(
        home,
        &mut live,
        &dir,
        &stage,
        &config_bytes,
        &rendered,
        &journal,
    ) {
        Ok(applied) => {
            clear_journal(home)?;
            let _ = std::fs::remove_dir_all(&stage);
            Ok(json!({"applied": true, "snapshot": dir, "record": applied, "plan": summary}))
        }
        Err(error) => match recover(home, &mut live, &dir, &journal) {
            Ok(()) => {
                clear_journal(home)?;
                let _ = std::fs::remove_dir_all(&stage);
                Err(error)
            }
            Err(unrecovered) => Err(invalid(format!(
                "{error}; recovery refused, the journal is kept: {unrecovered}"
            ))),
        },
    }
}

/// Refuses while another operation's journal is present.
fn require_no_journal(home: &Path) -> Result<()> {
    if journal_path(home).exists() {
        return require_current_store(home);
    }
    Ok(())
}

/// Publishes the staged target: replaces the database, writes the v2 config only over the exact
/// planned bytes, then the applied record. Called under the broker lock and
/// the database lease with the journal written.
fn publish(
    home: &Path,
    live: &mut Connection,
    snapshot: &Path,
    stage: &Path,
    config_bytes: &[u8],
    rendered: &str,
    journal: &Value,
) -> Result<Value> {
    let expect = |key: &str| journal[key].as_str().unwrap_or_default().to_owned();
    replace_db(live, &stage.join("state.db"))?;
    checkpoint("after_db")?;
    if std::fs::read(home.join("config.toml"))? != config_bytes {
        return Err(invalid(
            "config.toml changed during migration; its new content is kept",
        ));
    }
    fs::Dir::open(home)?.write(Path::new("config.toml"), rendered.as_bytes(), 0o600)?;
    checkpoint("after_config")?;
    let record = json!({
        "v2_config_sha256": fs::sha256(rendered.as_bytes()),
        "state_content_sha256": expect("target_state_sha256"),
        "applied_at": crate::domain::now(),
    });
    write_new(
        &snapshot.with_extension("applied.json"),
        &fs::canonical_json(&record)?,
    )?;
    Ok(record)
}

/// Returns the pair to the snapshot's original config and source database, touching
/// each side only while it provably is this operation's own state: the
/// config only when it equals the recorded v2 bytes (v1 is left alone, any
/// other content is a third-party edit and is kept), the database only when
/// every row equals the recorded target (a source-equal database is already
/// restored; any other content refuses). Refuses with active agents.
fn recover(home: &Path, live: &mut Connection, snapshot: &Path, journal: &Value) -> Result<()> {
    let expect = |key: &str| journal[key].as_str().unwrap_or_default().to_owned();
    let (digest, active) = live_state(live)?;
    if active > 0 {
        return Err(invalid("active agents must finish before rollback"));
    }
    let source = expect("source_state_sha256");
    if digest != source && digest != expect("target_state_sha256") {
        return Err(invalid(
            "state changed after migration (rows written); rollback cannot preserve them",
        ));
    }
    let current = fs::sha256(&std::fs::read(home.join("config.toml"))?);
    if current == expect("v2_config_sha256") {
        let v1 = std::fs::read(snapshot.join("config.v1.toml"))?;
        fs::Dir::open(home)?.write(Path::new("config.toml"), &v1, 0o600)?;
    }
    if digest != source {
        replace_db(live, &snapshot.join("state.db"))?;
        if live_state(live)?.0 != source {
            return Err(invalid("restored database does not match the snapshot"));
        }
    }
    Ok(())
}

/// `config rollback --snapshot DIR`: returns the home to the snapshot's
/// verified original config and schema-`source` database, and recovers an
/// interrupted apply or rollback of that snapshot.
///
/// Refuses unless the snapshot is complete and matches its manifest, the
/// recorded old release still verifies with its recorded binary and
/// `SHA256SUMS` digests, and there is proof of what this snapshot published:
/// its applied record or its own journal. The live config must equal the
/// snapshot's v1 or v2 bytes and every live row the recorded source or target
/// digest, with no active agent — a matching config alone never authorizes
/// replacing the database. The operation itself is journalled, so an
/// interrupted rollback is resumed by rerunning it.
pub fn rollback(home: &Path, snapshot: &Path) -> Result<Value> {
    let dir: PathBuf = snapshot.to_path_buf();
    if !dir.join("COMPLETE").is_file() {
        return Err(invalid("migration snapshot is incomplete"));
    }
    let manifest: Value = serde_json::from_slice(&std::fs::read(dir.join("manifest.json"))?)?;
    let expect = |value: &Value, key: &str| value[key].as_str().unwrap_or_default().to_owned();
    let v1 = std::fs::read(dir.join("config.v1.toml"))?;
    if fs::sha256(&v1) != expect(&manifest, "v1_config_sha256")
        || file_digest(&dir.join("state.db"))? != expect(&manifest, "state_backup_sha256")
    {
        return Err(invalid("migration snapshot does not match its manifest"));
    }
    let old = &manifest["old_release"];
    let release_dir = PathBuf::from(old["path"].as_str().unwrap_or_default());
    if old_release(&release_dir).ok().is_none_or(|now| {
        now["binary_sha256"] != old["binary_sha256"]
            || now["sha256sums_sha256"] != old["sha256sums_sha256"]
    }) {
        return Err(invalid(
            "the recorded pre-migration release is missing, unsealed or changed; restore it first",
        ));
    }
    let _lock = exclusive(home)?;
    let mut live = lease(home)?;
    let pending: Option<Value> = match std::fs::read(journal_path(home)) {
        Ok(bytes) => Some(serde_json::from_slice(&bytes)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if let Some(journal) = &pending {
        let owner = journal["snapshot"]
            .as_str()
            .and_then(|path| Path::new(path).canonicalize().ok());
        if owner.is_none() || owner != dir.canonicalize().ok() {
            return Err(invalid(
                "another snapshot's migration or rollback is unfinished; recover that one",
            ));
        }
    }
    let journal = match pending {
        Some(journal) => journal,
        None => {
            let applied: Value = serde_json::from_slice(
                &std::fs::read(dir.with_extension("applied.json"))
                    .map_err(|_| invalid("migration was never applied from this snapshot"))?,
            )?;
            json!({
                "operation": "rollback",
                "snapshot": dir,
                "source_state_sha256": manifest["state_content_sha256"],
                "target_state_sha256": applied["state_content_sha256"],
                "v1_config_sha256": manifest["v1_config_sha256"],
                "v2_config_sha256": manifest["v2_config_sha256"],
            })
        }
    };
    // Every precondition is checked before the journal is (re)written, so a
    // refusal leaves the home exactly as found.
    let current = fs::sha256(&std::fs::read(home.join("config.toml"))?);
    if current != expect(&journal, "v1_config_sha256")
        && current != expect(&journal, "v2_config_sha256")
    {
        return Err(invalid(
            "config.toml changed after migration; rollback would discard it",
        ));
    }
    let (digest, active) = live_state(&live)?;
    if active > 0 {
        return Err(invalid("active agents must finish before rollback"));
    }
    if digest != expect(&journal, "source_state_sha256")
        && digest != expect(&journal, "target_state_sha256")
    {
        return Err(invalid(
            "state changed after migration (rows written); rollback cannot preserve them",
        ));
    }
    checkpoint("rollback_checked")?;
    write_journal(home, &journal)?;
    recover(home, &mut live, &dir, &journal)?;
    clear_journal(home)?;
    drop(live);
    let _ = std::fs::remove_dir_all(dir.with_extension("stage"));
    Ok(json!({
        "rolled_back": true,
        "config_sha256": fs::sha256(&v1),
        "state_schema_version": manifest["source_schema_version"],
        "run_release": old,
    }))
}

#[cfg(test)]
mod tests {
    //! Preserve historical snapshot digests while removing whole-database allocations.
    use super::*;

    /// SQLite's binary sort must produce the exact old Rust Debug row framing for every value type.
    #[test]
    fn streamed_content_digest_matches_historical_format() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE entries(a,b); CREATE TABLE empty_table(value); INSERT INTO entries VALUES (NULL, X'00ff'), (2,'quotes\"'), (10,'строка'), (-2.5, 'line'||char(10)||'break'), (2,'quotes\"'), (0,'nul'||char(0)||'tail');").unwrap();
        let mut legacy = String::new();
        for table in ["empty_table", "entries"] {
            let mut statement = conn.prepare(&format!("SELECT * FROM {table}")).unwrap();
            let columns = statement.column_count();
            let mut query = statement.query([]).unwrap();
            let mut rows = Vec::new();
            while let Some(row) = query.next().unwrap() {
                rows.push(
                    (0..columns)
                        .map(|index| {
                            format!(
                                "{:?}\u{1f}",
                                row.get::<_, rusqlite::types::Value>(index).unwrap()
                            )
                        })
                        .collect::<String>(),
                );
            }
            rows.sort();
            legacy.push_str(&format!("{table}\u{1e}{}\u{1d}", rows.join("\u{1e}")));
        }
        assert_eq!(
            content_digest(&conn).unwrap(),
            fs::sha256(legacy.as_bytes())
        );
        conn.execute("INSERT INTO entries VALUES ('later', 3)", [])
            .unwrap();
        assert_ne!(
            content_digest(&conn).unwrap(),
            fs::sha256(legacy.as_bytes())
        );
    }
}
