//! Short-lived, thread-local SQLite connections. Never hold a transaction across await.
/// Atomic durable admission, replay, and active-capacity reservation.
pub mod admission;
/// Durable delivery outbox rows, binding, expiry, and evidence verdicts.
pub mod delivery;
/// Read-only diagnostic and active-context snapshots.
pub mod diagnostics;
/// Durable append-only journal operations and bounded transcript spooling.
pub mod journal;
/// Resume-parent proof and one-child lineage admission helpers.
pub mod lineage;
pub mod migrations;
/// Read projections, stable pages, and cursor-based transcript views.
pub mod projections;
/// Python-compatible normalization and repair of cumulative run usage rows.
pub mod run_stats;
/// Atomic terminal lifecycle transitions and their durable completion notices.
pub mod terminal;
use agent_run_domain::{
    catalog::{AccountId, PhysicalQuotaKey},
    domain::{self, now, AgentId, Outcome, StartRequest, Status},
    error::invalid,
    Error, Result,
};
use agent_run_platform::{fs, verify::Proof};
use rusqlite::{
    params, Connection, ErrorCode, OptionalExtension, Transaction, TransactionBehavior,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};
pub const VERSION: i64 = 17;
pub const ACTIVE_SQL: &str = "('created','starting','running','cancelling')";
/// How long an ordinary store connection waits out a competing writer before
/// giving up with `SQLITE_BUSY`.
///
/// Mirrors `PRAGMA busy_timeout=5000` in `src/agent_run/state/db.py`. It is
/// deliberately short: a normal caller blocked this long is reporting real
/// contention, and hiding that behind a longer wait would trade a fast, visible
/// failure for an unbounded stall.
pub(crate) const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the schema-migration connection waits out a competing writer.
///
/// Mirrors `PRAGMA busy_timeout=30000` in `src/agent_run/state/migrations.py`,
/// and is deliberately six times `BUSY_TIMEOUT`: migrations are serialized
/// across threads and processes by `migrations::SchemaLock`, so every extra
/// opener of an out-of-date store waits for all upgrades queued ahead of it.
/// During a rollout that queue routinely outlasts `BUSY_TIMEOUT`, and an
/// upgrade is the one write nobody can replay, so waiting beats failing.
pub(crate) const MIGRATION_BUSY_TIMEOUT: Duration = Duration::from_secs(30);
pub struct Store {
    pub conn: Connection,
    pub home: PathBuf,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub id: AgentId,
    pub request: StartRequest,
    pub status: Status,
    pub created_at: f64,
    pub started_at: Option<f64>,
    pub finished_at: Option<f64>,
    pub supervisor_pid: Option<i32>,
    pub supervisor_identity: Option<String>,
    pub supervisor_birth_time: Option<f64>,
    pub process_group_id: Option<i32>,
    pub runtime_session_id: Option<String>,
    pub failure_kind: Option<String>,
    pub failure_text: Option<String>,
    pub exit_code: Option<i32>,
    pub answer_path: Option<PathBuf>,
    pub answer_bytes: Option<u64>,
    pub answer_sha256: Option<String>,
    pub orchestrator_session_id: Option<String>,
    pub parent_agent_id: Option<AgentId>,
    pub root_agent_id: AgentId,
    pub sequence: u32,
    pub resume_of_runtime_session_id: Option<String>,
    pub identity: Option<Value>,
}

/// Captures persisted supervisor identity fields for immutable ownership checks.
type SupervisorOwnership = (
    String,
    Option<i32>,
    Option<String>,
    Option<i32>,
    Option<f64>,
);

/// Persisted fields used to decide whether a startup handoff may be renewed.
type StartupHandoff = (
    String,
    Option<String>,
    Option<f64>,
    Option<i32>,
    Option<i32>,
    Option<String>,
);

impl Record {
    /// Decodes one complete agents-table row without changing the database.
    pub(crate) fn read(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        fn parse<T: serde::de::DeserializeOwned>(v: String) -> rusqlite::Result<T> {
            serde_json::from_str(&v).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })
        }
        fn id(v: String) -> rusqlite::Result<AgentId> {
            v.parse().map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })
        }
        let agent_id = id(row.get("id")?)?;
        let root: String = row.get("root_agent_id")?;
        let status: String = row.get("status")?;
        let status = status.parse().map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?;
        Ok(Self {
            id: agent_id.clone(),
            request: parse(row.get("request_json")?)?,
            status,
            created_at: row.get("created_at")?,
            started_at: row.get("started_at")?,
            finished_at: row.get("finished_at")?,
            supervisor_pid: row.get("supervisor_pid")?,
            supervisor_identity: row.get("supervisor_identity")?,
            supervisor_birth_time: row.get("supervisor_birth_time")?,
            process_group_id: row.get("process_group_id")?,
            runtime_session_id: row.get("runtime_session_id")?,
            failure_kind: row.get("failure_kind")?,
            failure_text: row.get("failure_text")?,
            exit_code: row.get("exit_code")?,
            answer_path: row
                .get::<_, Option<String>>("answer_path")?
                .map(PathBuf::from),
            answer_bytes: row.get::<_, Option<i64>>("answer_bytes")?.map(|n| n as u64),
            answer_sha256: row.get("answer_sha256")?,
            orchestrator_session_id: row.get("orchestrator_session_id")?,
            parent_agent_id: row
                .get::<_, Option<String>>("parent_agent_id")?
                .map(id)
                .transpose()?,
            root_agent_id: if root.is_empty() { agent_id } else { id(root)? },
            sequence: row.get("sequence")?,
            resume_of_runtime_session_id: row.get("resume_of_runtime_session_id")?,
            identity: row
                .get::<_, Option<String>>("identity_json")?
                .map(parse)
                .transpose()?,
        })
    }
}
pub(crate) fn tx_event(
    tx: &Transaction<'_>,
    id: &AgentId,
    kind: &str,
    from: Option<Status>,
    to: Option<Status>,
    data: &Value,
) -> Result<i64> {
    tx.execute(
        "INSERT INTO events(agent_id,at,kind,from_status,to_status,data_json) VALUES(?,?,?,?,?,?)",
        params![
            id.as_str(),
            now(),
            kind,
            from.map(Status::as_str),
            to.map(Status::as_str),
            serde_json::to_string(data)?
        ],
    )?;
    Ok(tx.last_insert_rowid())
}

/// Finds or creates the normalized session row inside a caller-owned transaction.
///
/// Session identity deliberately excludes the external turn: later turns of
/// one chat update liveness and turn metadata without splitting its durable
/// agent and receipt scope.
fn session_for_reference(
    tx: &Transaction<'_>,
    reference: &domain::OrchestratorRef,
    at: f64,
) -> Result<String> {
    let candidate = format!("os-{}", uuid::Uuid::new_v4().simple());
    tx.execute(
        "INSERT INTO orchestrator_sessions(id,transport,external_session_id,external_turn_id,created_at,last_seen_at) VALUES(?,?,?,?,?,?) ON CONFLICT(transport,external_session_id) DO UPDATE SET last_seen_at=excluded.last_seen_at,external_turn_id=excluded.external_turn_id",
        params![candidate, reference.transport, reference.external_session_id, reference.external_turn_id, at, at],
    )?;
    Ok(tx.query_row(
        "SELECT id FROM orchestrator_sessions WHERE transport=? AND external_session_id=?",
        params![reference.transport, reference.external_session_id],
        |row| row.get(0),
    )?)
}

/// Decodes only the bounded versioned component receipt representation.
///
/// Malformed and pre-component receipt rows are legacy state and therefore
/// return `None`; the next visible context safely replaces them.
fn decode_context_components(key: &str) -> Option<BTreeMap<String, String>> {
    let value: Value = serde_json::from_str(key).ok()?;
    if value.get("v")?.as_i64()? != 2 {
        return None;
    }
    let components = value.get("components")?.as_object()?;
    if components.is_empty() {
        return None;
    }
    components
        .iter()
        .map(|(name, value)| {
            let value = value.as_str()?.to_owned();
            (!name.trim().is_empty() && !value.trim().is_empty()).then(|| (name.clone(), value))
        })
        .collect()
}

impl Store {
    /// Returns the canonical durable database path so another thread can open
    /// its own SQLite connection without violating connection affinity.
    pub fn path(&self) -> PathBuf {
        self.home.join("state.db")
    }

    pub fn initialize(home: &Path) -> Result<Self> {
        fs::private_dir(home)?;
        Self::connect(home, true)
    }
    pub fn open(home: &Path) -> Result<Self> {
        Self::connect(home, false)
    }
    /// Opens a thread-affine connection for `home`, migrating existing content
    /// and initializing a new schema when `create` is true. The connection
    /// enables WAL strictly: an already-WAL database skips conversion, while a
    /// competing delete-to-WAL conversion is retried briefly and other modes
    /// or SQLite errors are reported to the caller.
    fn connect(home: &Path, create: bool) -> Result<Self> {
        let path = home.join("state.db");
        if let Ok(meta) = std::fs::symlink_metadata(&path) {
            if !meta.is_file() || meta.file_type().is_symlink() {
                return Err(invalid("state.db must be a regular file"));
            }
        }
        if !create && !path.exists() {
            return Err(invalid("state.db is missing; run agent-run init"));
        }
        // A database that already has content is upgraded through the numbered
        // migration chain before this connection touches it, exactly as the
        // Python port's `open_database`/`initialize_database` call `migrate()`
        // first. A zero-byte file is left for the schema-init branch below.
        if path.exists() && path.metadata()?.len() > 0 {
            migrations::migrate(&path)?;
        }
        let conn = Connection::open(&path)?;
        conn.busy_timeout(BUSY_TIMEOUT)?;
        conn.pragma_update(None, "foreign_keys", true)?;
        let version: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if version == 0 && create {
            // Concurrent first initializers must converge on one committed
            // schema. The same cross-process lock the migration chain uses
            // serializes creation, and the version and table count are re-read
            // inside it so a loser of the race adopts the winner's schema
            // instead of replaying schema.sql onto tables that now exist.
            let _lock = migrations::SchemaLock::acquire(&path)?;
            let version: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
            let tables: i64 = conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table'",
                [],
                |r| r.get(0),
            )?;
            if version == 0 {
                if tables != 0 {
                    return Err(invalid(
                        "unversioned nonempty database is not safe to initialize",
                    ));
                }
                conn.execute_batch("BEGIN IMMEDIATE")?;
                if let Err(e) = conn.execute_batch(include_str!("../../../sql/schema.sql")) {
                    let _ = conn.execute_batch("ROLLBACK");
                    return Err(e.into());
                }
                conn.execute_batch("COMMIT")?;
            } else if version != VERSION {
                return Err(invalid(
                    "state database has no usable schema; run agent-run init",
                ));
            }
        } else if version != VERSION {
            // `migrations::migrate` above already brings any 1..VERSION store up
            // to date or refuses a newer one, so this is now a defensive check:
            // it only fires for a versionless (0) store opened with `create:
            // false`, which has no schema for this connection to use.
            return Err(invalid(if version > VERSION {
                "database schema is newer than this binary"
            } else {
                "state database has no usable schema; run agent-run init"
            }));
        }
        let journal_mode: String =
            conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            for attempt in 0..8 {
                match conn.pragma_update_and_check(None, "journal_mode", "WAL", |row| {
                    row.get::<_, String>(0)
                }) {
                    Ok(mode) if mode.eq_ignore_ascii_case("wal") => break,
                    Ok(mode) => {
                        return Err(invalid(format!(
                            "state database could not enable WAL mode: {mode}"
                        )))
                    }
                    Err(error)
                        if attempt < 7
                            && matches!(
                                error.sqlite_error_code(),
                                Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
                            ) =>
                    {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        conn.pragma_update(None, "synchronous", "FULL")?;
        // Tightening modes is strict only for the creator: a store left group- or
        // world-readable at creation is a defect. For a store that already
        // existed the chmod is best effort, so a sandboxed reader that may not
        // chmod a file it does not own still gets a connection. This mirrors
        // Python's `_private_path(strict=not existed)` in state/db.py, including
        // its tightening of the WAL and SHM siblings and its tolerance of a
        // sibling that does not exist yet.
        use std::os::unix::fs::PermissionsExt;
        let private = std::fs::Permissions::from_mode(0o600);
        for candidate in [
            path.clone(),
            PathBuf::from(format!("{}-wal", path.display())),
            PathBuf::from(format!("{}-shm", path.display())),
        ] {
            match std::fs::set_permissions(&candidate, private.clone()) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) if create => return Err(error.into()),
                Err(_) => {}
            }
        }
        Ok(Self {
            conn,
            home: home.to_path_buf(),
        })
    }
    pub fn health(&self) -> Result<Value> {
        let version: i64 = self
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        let integrity: String = self
            .conn
            .query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        let tables: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        Ok(
            json!({"ok":version==VERSION&&integrity=="ok","schema_version":version,"integrity":integrity,"tables":tables}),
        )
    }
    pub fn backup(&self, destination: &Path) -> Result<()> {
        if destination.exists() {
            return Err(invalid("backup destination already exists"));
        }
        self.conn
            .backup(rusqlite::DatabaseName::Main, destination, None)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o600))?;
        Ok(())
    }
    pub fn get(&self, id: &AgentId) -> Result<Record> {
        self.conn
            .query_row(
                "SELECT * FROM agents WHERE id=?",
                [id.as_str()],
                Record::read,
            )
            .optional()?
            .ok_or_else(|| Error::NotFound(id.to_string()))
    }
    pub fn event(&self, id: &AgentId, kind: &str, data: &Value) -> Result<()> {
        self.conn.execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,?,?)",
            params![id.as_str(), now(), kind, serde_json::to_string(data)?],
        )?;
        Ok(())
    }
    /// Binds an existing durable agent to one immutable orchestrator session.
    ///
    /// The supplied reference is validated and upserted atomically after the
    /// agent lookup, so an unknown agent cannot leave a stray session row.
    /// Repeating the same binding succeeds; a different session is rejected.
    /// A waiting terminal delivery is activated exactly once in that same
    /// transaction and is never resurrected after it has progressed.
    pub fn bind_orchestrator(
        &mut self,
        id: &AgentId,
        reference: &domain::OrchestratorRef,
        at: f64,
    ) -> Result<String> {
        reference.validate()?;
        if !at.is_finite() || at < 0.0 {
            return Err(invalid("binding time must be finite and nonnegative"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let agent = tx
            .query_row(
                "SELECT * FROM agents WHERE id=?",
                [id.as_str()],
                Record::read,
            )
            .optional()?
            .ok_or_else(|| Error::NotFound(id.to_string()))?;
        let session_id = session_for_reference(&tx, reference, at)?;
        if let Some(current) = agent.orchestrator_session_id {
            if current != session_id {
                return Err(invalid("agent orchestration binding is immutable"));
            }
        } else {
            tx.execute(
                "UPDATE agents SET orchestrator_session_id=? WHERE id=?",
                params![session_id, id.as_str()],
            )?;
            tx.execute(
                "UPDATE deliveries SET orchestrator_session_id=?,state='pending',next_attempt_at=? WHERE agent_id=? AND state='waiting_binding'",
                params![session_id, at, id.as_str()],
            )?;
        }
        tx.commit()?;
        Ok(session_id)
    }

    /// Finds the existing durable session for a reference without creating one.
    pub fn find_orchestrator_session(
        &self,
        reference: &domain::OrchestratorRef,
    ) -> Result<Option<String>> {
        reference.validate()?;
        Ok(self
            .conn
            .query_row(
                "SELECT id FROM orchestrator_sessions WHERE transport=? AND external_session_id=?",
                params![reference.transport, reference.external_session_id],
                |row| row.get(0),
            )
            .optional()?)
    }

    /// Atomically records visible context component fingerprints for a session reference.
    ///
    /// Only changed names are returned.  The JSON versioned representation
    /// preserves fingerprints for omitted components, while legacy plain keys
    /// intentionally compare as empty and are upgraded in place.
    pub fn record_context_components_for_ref(
        &mut self,
        reference: &domain::OrchestratorRef,
        components: &BTreeMap<String, String>,
        at: f64,
    ) -> Result<(String, Vec<String>)> {
        reference.validate()?;
        if components.is_empty()
            || components
                .iter()
                .any(|(name, value)| name.trim().is_empty() || value.trim().is_empty())
        {
            return Err(invalid("context components must be nonblank"));
        }
        if !at.is_finite() || at < 0.0 {
            return Err(invalid("context time must be finite and nonnegative"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session_id = session_for_reference(&tx, reference, at)?;
        let stored: BTreeMap<String, String> = tx
            .query_row(
                "SELECT context_key FROM context_receipts WHERE orchestrator_session_id=?",
                [&session_id],
                |row| row.get(0),
            )
            .optional()?
            .and_then(|key: String| decode_context_components(&key))
            .unwrap_or_default();
        let changed: Vec<String> = components
            .iter()
            .filter(|(name, value)| stored.get(*name) != Some(*value))
            .map(|(name, _)| name.clone())
            .collect();
        if !changed.is_empty() {
            let mut merged = stored;
            merged.extend(components.clone());
            let key = serde_json::to_string(&json!({"v": 2, "components": merged}))?;
            tx.execute(
                "INSERT INTO context_receipts(orchestrator_session_id,context_key,injected_at) VALUES(?,?,?) ON CONFLICT(orchestrator_session_id) DO UPDATE SET context_key=excluded.context_key,injected_at=excluded.injected_at",
                params![session_id, key, at],
            )?;
        }
        tx.commit()?;
        Ok((session_id, changed))
    }

    /// Upserts one legacy-compatible context receipt only when its key changed.
    pub fn record_context_receipt(
        &mut self,
        session_id: &str,
        context_key: &str,
        at: f64,
    ) -> Result<bool> {
        if session_id.trim().is_empty() || context_key.trim().is_empty() {
            return Err(invalid("context receipt identifiers must be nonblank"));
        }
        if !at.is_finite() || at < 0.0 {
            return Err(invalid("context time must be finite and nonnegative"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "INSERT INTO context_receipts(orchestrator_session_id,context_key,injected_at) VALUES(?,?,?) ON CONFLICT(orchestrator_session_id) DO UPDATE SET context_key=excluded.context_key,injected_at=excluded.injected_at WHERE context_receipts.context_key<>excluded.context_key",
            params![session_id, context_key, at],
        )? == 1;
        tx.commit()?;
        Ok(changed)
    }

    /// Finds or creates a session for a reference, then upserts its context key.
    pub fn record_context_receipt_for_ref(
        &mut self,
        reference: &domain::OrchestratorRef,
        context_key: &str,
        at: f64,
    ) -> Result<(String, bool)> {
        reference.validate()?;
        if context_key.trim().is_empty() {
            return Err(invalid("context key must be nonblank"));
        }
        if !at.is_finite() || at < 0.0 {
            return Err(invalid("context time must be finite and nonnegative"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session_id = session_for_reference(&tx, reference, at)?;
        let changed = tx.execute(
            "INSERT INTO context_receipts(orchestrator_session_id,context_key,injected_at) VALUES(?,?,?) ON CONFLICT(orchestrator_session_id) DO UPDATE SET context_key=excluded.context_key,injected_at=excluded.injected_at WHERE context_receipts.context_key<>excluded.context_key",
            params![session_id, context_key, at],
        )? == 1;
        tx.commit()?;
        Ok((session_id, changed))
    }
    pub fn set_owner(
        &self,
        id: &AgentId,
        pid: i32,
        identity: &str,
        birth: Option<f64>,
    ) -> Result<()> {
        let n=self.conn.execute("UPDATE agents SET supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=? WHERE id=? AND status='starting' AND supervisor_pid IS NULL",params![pid,identity,birth,id.as_str()])?;
        if n != 1 {
            return Err(Error::Conflict);
        }
        self.event(id, "supervisor_ready", &json!({"pid":pid}))
    }

    /// Claims one starting admission for a coordinator until its bounded handoff deadline.
    ///
    /// The diagnostic owner string and optional process birth proof are immutable while the
    /// row remains starting. Repeating the exact claim is idempotent; a terminal, non-starting,
    /// or differently owned row is rejected without a partial update.
    pub fn claim_startup(
        &mut self,
        id: &AgentId,
        owner_identity: &str,
        owner_birth_time: Option<f64>,
        at: f64,
        deadline_seconds: f64,
    ) -> Result<()> {
        domain::nonblank("startup owner identity", owner_identity)?;
        if !at.is_finite() || at < 0.0 {
            return Err(invalid("startup claim time must be finite and nonnegative"));
        }
        if owner_birth_time.is_some_and(|birth| !birth.is_finite() || birth < 0.0) {
            return Err(invalid(
                "startup owner birth time must be finite and nonnegative",
            ));
        }
        if !deadline_seconds.is_finite() || deadline_seconds <= 0.0 {
            return Err(invalid("startup deadline must be positive and finite"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: Option<(String, Option<String>, Option<f64>)> = tx
            .query_row(
                "SELECT status,startup_owner_pid_identity,startup_owner_birth_time FROM agents WHERE id=?",
                [id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((status, current_owner, current_birth)) = current else {
            return Err(Error::NotFound(id.to_string()));
        };
        if status != Status::Starting.as_str() {
            return Err(Error::Transition(
                "startup owner requires starting agent".into(),
            ));
        }
        if current_owner.is_some() {
            if current_owner.as_deref() != Some(owner_identity) || current_birth != owner_birth_time
            {
                return Err(Error::Transition("startup is already owned".into()));
            }
        } else {
            tx.execute(
                "UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=?,startup_deadline_at=? WHERE id=?",
                params![owner_identity, owner_birth_time, at + deadline_seconds, id.as_str()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Extends a still-live coordinator claim for the supervisor READY handoff.
    ///
    /// The matching owner must still be starting, unbound, and before its existing deadline.
    /// A successful call replaces that deadline with `at + deadline_seconds`; elapsed time
    /// alone is never reconciliation proof.
    pub fn begin_supervisor_handoff(
        &mut self,
        id: &AgentId,
        owner_identity: &str,
        at: f64,
        deadline_seconds: f64,
    ) -> Result<bool> {
        domain::nonblank("startup owner identity", owner_identity)?;
        if !at.is_finite() || at < 0.0 {
            return Err(invalid("handoff time must be finite and nonnegative"));
        }
        if !deadline_seconds.is_finite() || deadline_seconds <= 0.0 {
            return Err(invalid("startup deadline must be positive and finite"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<StartupHandoff> = tx
            .query_row(
                "SELECT status,startup_owner_pid_identity,startup_deadline_at,supervisor_pid,process_group_id,supervisor_identity FROM agents WHERE id=?",
                [id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
            )
            .optional()?;
        let Some((status, owner, deadline, pid, group, identity)) = row else {
            return Err(Error::NotFound(id.to_string()));
        };
        let eligible = status == Status::Starting.as_str()
            && owner.as_deref() == Some(owner_identity)
            && deadline.is_some_and(|value| value > at)
            && pid.is_none()
            && group.is_none()
            && identity.is_none();
        if !eligible {
            tx.commit()?;
            return Ok(false);
        }
        tx.execute(
            "UPDATE agents SET startup_deadline_at=? WHERE id=?",
            params![at + deadline_seconds, id.as_str()],
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// Records immutable supervisor identity and permits one process-group refinement.
    pub fn record_supervisor(
        &mut self,
        id: &AgentId,
        pid: i32,
        identity: &str,
        process_group_id: i32,
        birth_time: Option<f64>,
        at: f64,
    ) -> Result<()> {
        if pid <= 0
            || process_group_id <= 0
            || identity.trim().is_empty()
            || !at.is_finite()
            || at < 0.0
        {
            return Err(invalid("invalid supervisor ownership"));
        }
        if birth_time.is_some_and(|birth| !birth.is_finite() || birth < 0.0) {
            return Err(invalid(
                "supervisor birth time must be finite and nonnegative",
            ));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: Option<SupervisorOwnership> = tx
            .query_row("SELECT status,supervisor_pid,supervisor_identity,process_group_id,supervisor_birth_time FROM agents WHERE id=?", [id.as_str()], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)))
            .optional()?;
        let Some((status, old_pid, old_identity, old_group, old_birth)) = current else {
            return Err(Error::NotFound(id.to_string()));
        };
        if status.parse::<Status>()?.terminal() {
            return Err(Error::Transition(
                "terminal agent cannot bind a supervisor".into(),
            ));
        }
        if let Some(old_pid) = old_pid {
            if old_pid != pid
                || old_identity.as_deref() != Some(identity)
                || old_birth != birth_time
                || (old_group.is_some()
                    && old_group != Some(process_group_id)
                    && old_group != Some(pid))
            {
                return Err(invalid("supervisor ownership is immutable"));
            }
        }
        tx.execute(
            "UPDATE agents SET supervisor_pid=?,supervisor_identity=?,process_group_id=?,supervisor_birth_time=?,heartbeat_at=? WHERE id=? AND status IN ('starting','running')",
            params![pid, identity, process_group_id, birth_time, at, id.as_str()],
        )?;
        tx.execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,?,?)",
            params![
                id.as_str(),
                at,
                "supervisor_ready",
                json!({"pid":pid}).to_string()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn update_identity(&self, id: &AgentId, identity: &Value, revision: &str) -> Result<()> {
        let n = self.conn.execute(
            "UPDATE agents SET identity_json=?,config_revision=? WHERE id=? AND status='starting'",
            params![serde_json::to_string(identity)?, revision, id.as_str()],
        )?;
        if n != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    pub fn running(&mut self, id: &AgentId, pgid: i32) -> Result<()> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let s: String =
            tx.query_row("SELECT status FROM agents WHERE id=?", [id.as_str()], |r| {
                r.get(0)
            })?;
        let from: Status = s.parse()?;
        from.transition(Status::Running)?;
        tx.execute(
            "UPDATE agents SET status='running',started_at=?,process_group_id=? WHERE id=?",
            params![now(), pgid, id.as_str()],
        )?;
        tx.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at) VALUES(?,?,1,'running','{}',?)",params![format!("{}:1",id),id.as_str(),now()])?;
        tx_event(
            &tx,
            id,
            "status",
            Some(from),
            Some(Status::Running),
            &json!({}),
        )?;
        tx.commit()?;
        Ok(())
    }
    /// Returns the committed global revision of the entire quota snapshot.
    ///
    /// Admission reads this inside `BEGIN IMMEDIATE` and compares it with
    /// the producer's candidate revision. The store never scores candidates.
    pub fn quota_capacity_revision(&self) -> Result<i64> {
        Self::quota_capacity_revision_in(&self.conn)
    }
    /// Reads the same singleton inside a producer or admission transaction.
    pub fn quota_capacity_revision_in(conn: &Connection) -> Result<i64> {
        Ok(conn.query_row(
            "SELECT revision FROM quota_capacity_revision WHERE id=1",
            [],
            |row| row.get(0),
        )?)
    }
    /// Advances the global snapshot revision inside the caller's transaction.
    ///
    /// Collectors must write every relevant quota fact and call this once
    /// before committing that same transaction, preserving snapshot atomicity.
    pub fn advance_quota_capacity_revision(tx: &Transaction<'_>) -> Result<i64> {
        let changed = tx.execute(
            "UPDATE quota_capacity_revision SET revision=revision+1,updated_at=? \
             WHERE id=1 AND revision<9223372036854775807",
            [now()],
        )?;
        if changed != 1 {
            return Err(invalid("quota capacity revision cannot advance"));
        }
        Self::quota_capacity_revision_in(tx)
    }
    /// Counts active reservations for each exact physical pool, including
    /// shared aliases and attempts consuming multiple pools.
    pub fn active_reservation_counts(
        &self,
        keys: &[PhysicalQuotaKey],
    ) -> Result<BTreeMap<String, u64>> {
        let mut counts = BTreeMap::new();
        for key in keys {
            let open: i64 = self.conn.query_row(
                "SELECT COUNT(*) FROM attempt_quota_keys k JOIN attempts a ON a.id=k.attempt_id \
                 WHERE k.quota_key=? AND a.ownership_active=1",
                [key.as_str()],
                |row| row.get(0),
            )?;
            counts.insert(key.as_str().to_owned(), open as u64);
        }
        Ok(counts)
    }
    /// Counts owned orchestrated attempts per selected global account.
    ///
    /// Only attempts that carry a selected account and currently hold the
    /// ownership flag are counted; released and legacy attempts stay
    /// invisible, matching the schema's one-active-attempt partial index on
    /// `ownership_active`. Accounts absent from `accounts` are not queried;
    /// absent rows count as zero.
    pub fn active_attempt_counts(&self, accounts: &[AccountId]) -> Result<BTreeMap<String, u64>> {
        let mut counts = BTreeMap::new();
        for account in accounts {
            let open: i64 = self.conn.query_row(
                "SELECT COUNT(*) FROM attempts WHERE selected_account_id=? AND ownership_active=1",
                [account.as_str()],
                |row| row.get(0),
            )?;
            counts.insert(account.as_str().to_owned(), open as u64);
        }
        Ok(counts)
    }
    /// Records a native runtime session and its durable session event atomically.
    ///
    /// The connection remains thread-affine; callers on another thread must open their own
    /// [`Store`] from the same home. Repeated calls retain the latest session identity and
    /// append the corresponding event for the lifecycle journal.
    pub fn runtime_session(&mut self, id: &AgentId, session: &str) -> Result<()> {
        domain::external_id("runtime_session_id", session)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE agents SET runtime_session_id=? WHERE id=?",
            params![session, id.as_str()],
        )?;
        tx.execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,?,?)",
            params![
                id.as_str(),
                now(),
                "runtime_session",
                json!({"id":session}).to_string()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn message(
        &self,
        id: &AgentId,
        role: &str,
        text: &str,
        name: Option<&str>,
        raw_ref: Option<&str>,
    ) -> Result<()> {
        if text.is_empty() {
            return Ok(());
        }
        if !["user", "assistant", "system", "tool_call", "tool_result"].contains(&role) {
            return Err(invalid("unknown transcript role"));
        }
        let (content, raw_ref) = journal::message_storage(&self.home, id, text, raw_ref)?;
        self.conn.execute(
            "INSERT INTO messages(agent_id,at,role,name,content,raw_ref) VALUES(?,?,?,?,?,?)",
            params![id.as_str(), now(), role, name, content, raw_ref],
        )?;
        Ok(())
    }
    pub fn finish(
        &mut self,
        id: &AgentId,
        outcome: &Outcome,
        proof: Option<&Proof>,
        usage: Option<&Value>,
    ) -> Result<()> {
        terminal::finish(self, id, outcome, proof, usage)
    }
    /// Recomputes one agent's normalized run statistics from durable events.
    ///
    /// The replacement is idempotent and retains `NULL` for measurements the
    /// engine did not report.
    pub fn record_run_stats(&mut self, id: &AgentId) -> Result<()> {
        run_stats::record(self, id)
    }
    /// Fills only missing statistics rows and returns `(backfilled, skipped)`.
    ///
    /// An individual malformed or unavailable agent does not stop later rows
    /// from being repaired; its count is returned as `skipped`.
    pub fn backfill_run_stats(&mut self) -> Result<(usize, usize)> {
        run_stats::backfill(self)
    }
    pub fn enqueue(&mut self, id: &AgentId, kind: &str, payload: &Value) -> Result<Value> {
        if !["cancel", "steer"].contains(&kind) {
            return Err(invalid("unknown command kind"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = tx
            .query_row(
                "SELECT * FROM agents WHERE id=?",
                [id.as_str()],
                Record::read,
            )
            .optional()?
            .ok_or_else(|| Error::NotFound(id.to_string()))?;
        if row.status.terminal() {
            return Err(invalid("agent is already terminal"));
        }
        tx.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,?,?,'pending',?)",params![id.as_str(),kind,serde_json::to_string(payload)?,now()])?;
        let cid = tx.last_insert_rowid();
        tx.commit()?;
        Ok(json!({"command_id":cid,"agent_id":id,"kind":kind,"state":"pending"}))
    }
    pub fn cancel_pending(&self, id: &AgentId) -> Result<bool> {
        Ok(self.conn.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE agent_id=? AND kind='cancel' AND state IN ('pending','claimed'))",[id.as_str()],|r|r.get(0))?)
    }
    pub fn revision(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COALESCE(MAX(seq),0) FROM events", [], |r| r.get(0))?)
    }
    pub fn list(
        &self,
        active: bool,
        offset: usize,
        limit: usize,
        session: Option<&domain::OrchestratorRef>,
    ) -> Result<(Vec<Record>, i64)> {
        if limit == 0 || limit > 1000 {
            return Err(invalid("limit must be 1..1000"));
        }
        let sid=match session{Some(s)=>self.conn.query_row("SELECT id FROM orchestrator_sessions WHERE transport=? AND external_session_id=?",params![s.transport,s.external_session_id],|r|r.get::<_,String>(0)).optional()?,None=>None};
        if session.is_some() && sid.is_none() {
            return Ok((vec![], 0));
        }
        let where_sql = format!(
            "WHERE (?=0 OR status IN {ACTIVE_SQL}) AND (? IS NULL OR orchestrator_session_id=?)"
        );
        let total = self.conn.query_row(
            &format!("SELECT COUNT(*) FROM agents {where_sql}"),
            params![active, sid, sid],
            |r| r.get(0),
        )?;
        let mut stmt = self.conn.prepare(&format!(
            "SELECT * FROM agents {where_sql} ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?"
        ))?;
        let rows = stmt
            .query_map(
                params![active, sid, sid, limit as i64, offset as i64],
                Record::read,
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok((rows, total))
    }
    pub fn transcript(&self, id: &AgentId, cursor: i64, limit: usize) -> Result<Value> {
        self.get(id)?;
        if cursor < 0 || limit == 0 || limit > 1000 {
            return Err(invalid("invalid transcript cursor or limit"));
        }
        let mut stmt=self.conn.prepare("SELECT seq,at,role,name,content,raw_ref FROM messages WHERE agent_id=? AND seq>? ORDER BY seq LIMIT ?")?;
        let mut rows = stmt.query(params![id.as_str(), cursor, limit as i64 + 1])?;
        let mut messages = Vec::new();
        let mut bytes = 0;
        let mut more = false;
        while let Some(row) = rows.next()? {
            let content: String = row.get(4)?;
            if messages.len() >= limit
                || (!messages.is_empty() && bytes + content.len() > 256 * 1024)
            {
                more = true;
                break;
            }
            bytes += content.len();
            messages.push(json!({"seq":row.get::<_,i64>(0)?,"at":row.get::<_,f64>(1)?,"role":row.get::<_,String>(2)?,"name":row.get::<_,Option<String>>(3)?,"content":content,"raw_ref":row.get::<_,Option<String>>(5)?}));
        }
        let next = if more {
            messages.last().and_then(|m| m.get("seq")).cloned()
        } else {
            None
        };
        Ok(
            json!({"agent_id":id,"messages":messages,"cursor":cursor,"limit":limit,"next_cursor":next,"complete":!more}),
        )
    }
    pub fn delivery_status(&self, id: &AgentId) -> Result<Value> {
        let row = self.get(id)?;
        let d=self.conn.query_row("SELECT id,state,attempts,ambiguous_result,last_error FROM deliveries WHERE agent_id=? ORDER BY terminal_event_seq DESC LIMIT 1",[id.as_str()],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,u32>(2)?,r.get::<_,bool>(3)?,r.get::<_,Option<String>>(4)?))).optional()?;
        let (notification_id, state, attempts, ambiguous, last_error, last_attempt) = if let Some(
            (did, state, attempts, ambiguous, last_error),
        ) = d
        {
            let evidence=self.conn.query_row("SELECT evidence_json FROM delivery_attempt_evidence WHERE delivery_id=? ORDER BY attempt DESC LIMIT 1",[&did],|r|r.get::<_,String>(0)).optional()?;
            (
                Some(did),
                state,
                attempts,
                ambiguous,
                last_error,
                evidence
                    .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                    .and_then(|v| safe_evidence(&v)),
            )
        } else {
            (None, "not_created".into(), 0, false, None, None)
        };
        Ok(
            json!({"agent_id":id,"bound":row.orchestrator_session_id.is_some(),"orchestrator_session_id":row.orchestrator_session_id,"notification_id":notification_id,"state":state,"attempts":attempts,"ambiguous":ambiguous,"last_error":last_error,"last_attempt":last_attempt}),
        )
    }

    /// Claims the oldest due outbox row or an expired sending lease.
    pub fn claim_delivery(
        &mut self,
        owner: &str,
        at: f64,
        lease_seconds: f64,
    ) -> Result<Option<Value>> {
        delivery::claim(self, owner, at, lease_seconds)
    }

    /// Completes one owned outbox row and optionally stores its attempt evidence.
    pub fn complete_delivery(
        &mut self,
        delivery_id: &str,
        owner: &str,
        at: f64,
        remote_message_id: Option<&str>,
        ambiguous: bool,
        evidence: Option<&Value>,
    ) -> Result<()> {
        delivery::complete(
            self,
            delivery_id,
            owner,
            at,
            remote_message_id,
            ambiguous,
            evidence,
        )
    }

    /// Permanently fails one owned outbox row and optionally stores its evidence.
    pub fn fail_delivery(
        &mut self,
        delivery_id: &str,
        owner: &str,
        error: &str,
        at: f64,
        ambiguous: bool,
        evidence: Option<&Value>,
    ) -> Result<()> {
        delivery::fail(self, delivery_id, owner, error, at, ambiguous, evidence)
    }

    /// Schedules one owned outbox retry with bounded exponential backoff.
    #[allow(clippy::too_many_arguments)] // Mirrors the explicit Python retry contract.
    pub fn retry_delivery(
        &mut self,
        delivery_id: &str,
        owner: &str,
        error: &str,
        at: f64,
        ambiguous: bool,
        evidence: Option<&Value>,
        base_delay: f64,
        max_delay: f64,
    ) -> Result<f64> {
        delivery::retry(
            self,
            delivery_id,
            owner,
            error,
            at,
            ambiguous,
            evidence,
            base_delay,
            max_delay,
        )
    }

    /// Returns the latest validated immutable evidence document for a delivery.
    pub fn latest_delivery_attempt(&self, delivery_id: &str) -> Result<Option<Value>> {
        delivery::latest(self, delivery_id)
    }
    pub fn last_progress(&self, id: &AgentId) -> Result<Option<f64>> {
        Ok(self.conn.query_row(
            "SELECT MAX(at) FROM messages WHERE agent_id=?",
            [id.as_str()],
            |r| r.get(0),
        )?)
    }
    pub fn last_event(&self, id: &AgentId, kind: &str) -> Result<Option<Value>> {
        let raw=self.conn.query_row("SELECT data_json FROM events WHERE agent_id=? AND kind=? ORDER BY seq DESC LIMIT 1",params![id.as_str(),kind],|r|r.get::<_,String>(0)).optional()?;
        raw.map(|s| serde_json::from_str(&s).map_err(Error::from))
            .transpose()
    }
}

/// Returns a delivery evidence document only when its classifier is safe to expose.
fn safe_evidence(raw: &Value) -> Option<Value> {
    let classifier = raw.get("classifier")?.as_str()?;
    if [
        "relay_accepted",
        "relay_rejected",
        "relay_unavailable",
        "relay_ambiguous",
        "uds_written",
        "session_gone",
        "uds_unavailable",
        "uds_ambiguous",
        "unsupported_transport",
        "delivery_expired",
    ]
    .contains(&classifier)
    {
        Some(raw.clone())
    } else {
        None
    }
}
