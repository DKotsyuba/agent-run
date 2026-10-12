//! Write authority, fences and read-only access of the single resident writer.
//!
//! Exactly one online process (the resident broker's writer thread) holds the
//! home-wide [`crate::writer::OnlineWriterFence`] and the one read-write [`crate::writer::StoreWriter`]
//! connection. Offline tools obtain [`crate::writer::OfflineWriterGuard`] through the same
//! lease plus a [`crate::writer::BrokerExclusion`] and a quiescence proof. Every other online
//! handle is a [`crate::writer::StoreReader`]: read-only, `query_only`, never creating or
//! migrating a database. [`crate::writer::WriteTx`] nests store helpers inside the writer's
//! single top-level transaction without independent commits.

use crate::{ACTIVE_SQL, Store, VERSION};
use agent_run_domain::{
    Error, Result,
    domain::now,
    error::invalid,
    storage::{MaintenanceOutcome, MonoDeadline, SubmitResult, WriteAuthority},
};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::{
    fs::{File, OpenOptions},
    ops::Deref,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// tokio-rusqlite adapter for the existing fenced, typed SQL units.
pub mod queued;

/// Busy wait of the sole writer connection. Any wait means an unknown writer
/// holds the WAL write lock, which the writer reports instead of hiding.
pub const WRITER_BUSY: Duration = Duration::from_millis(250);
/// SQL function registered only on authorized read-write connections; the
/// schema's mutation-fence triggers call it, so unregistered (legacy or
/// accidental) connections cannot change application rows.
pub const FENCE_FUNCTION: &str = "agent_run_writer_fence";

/// One nested write scope on a connection.
///
/// At depth 0 it issues `BEGIN IMMEDIATE` and `commit` issues `COMMIT`; when
/// a transaction is already open it uses a uniquely named `SAVEPOINT`, and
/// `commit` only `RELEASE`s it (never a durability claim). Dropping without
/// `commit` rolls back exactly its own scope. Store helpers use this instead
/// of opening independent transactions, so the writer's receipt and mutation
/// commit together.
pub struct WriteTx<'c>(sqlite_access::WriteScope<'c>);

impl<'c> WriteTx<'c> {
    /// Begin an outer IMMEDIATE or nested SQL scope; preserve typed SQLite errors.
    pub fn begin(conn: &'c Connection) -> Result<Self> {
        sqlite_access::WriteScope::begin(conn)
            .map(Self)
            .map_err(Into::into)
    }
    /// Commit/release this scope; nested release is not a durability receipt.
    pub fn commit(self) -> Result<()> {
        self.0.commit().map_err(Into::into)
    }
    /// Whether this scope owns the actual outer transaction.
    pub fn is_top_level(&self) -> bool {
        self.0.is_top_level()
    }
}
impl Deref for WriteTx<'_> {
    type Target = Connection;
    /// The same scoped connection; rollback lifetime belongs to the inner guard.
    fn deref(&self) -> &Connection {
        &self.0
    }
}

/// Opens or creates a private lock file `name` in `home`, refusing symlinks
/// and files owned by another user.
fn lock_file(home: &Path, name: &str) -> Result<File> {
    let path = home.join(name);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)?;
    let meta = file.metadata()?;
    // SAFETY: getuid has no preconditions and cannot fail.
    let uid = unsafe { libc::getuid() };
    if !meta.is_file() || meta.uid() != uid {
        return Err(invalid(format!("{name} is not a private owned file")));
    }
    Ok(file)
}

/// Takes a non-blocking exclusive flock; contention is `Error::Conflict`.
fn try_exclusive(file: &File) -> Result<()> {
    use fs2::FileExt;
    file.try_lock_exclusive().map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock
            || error.raw_os_error() == Some(libc::EWOULDBLOCK)
        {
            Error::Conflict
        } else {
            Error::Io(error)
        }
    })
}

/// Canonical existing home directory (symlinks resolved). Every lease, lock
/// and database path is derived from it, so a proof for one home can never
/// authorize another.
fn canonical_home(home: &Path) -> Result<PathBuf> {
    let canonical = std::fs::canonicalize(home)?;
    if !canonical.is_dir() {
        return Err(invalid("home is not a directory"));
    }
    Ok(canonical)
}

/// The state database path of a canonical home when it exists as a regular,
/// non-symlink file; `None` when absent. A symlink or other file type is
/// refused before anything opens it.
fn state_path(home: &Path) -> Result<Option<PathBuf>> {
    let path = home.join("state.db");
    match std::fs::symlink_metadata(&path) {
        Ok(meta) if meta.file_type().is_symlink() || !meta.is_file() => {
            Err(invalid("state.db must be a regular file"))
        }
        Ok(_) => Ok(Some(path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Namespace of the home-wide writer lease (`<home>/writer.lock`).
pub struct WriterLease;

/// Preserve the application's typed contention error from the reusable file lock.
fn lease_error(error: std::io::Error) -> Error {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        Error::Conflict
    } else {
        Error::Io(error)
    }
}

impl WriterLease {
    /// Acquires the online writer lease of the canonical `home` without
    /// blocking. Another holder (a live broker writer or an offline tool)
    /// yields `Error::Conflict`. The lease lasts until the returned fence (or
    /// the writer that consumed it) is dropped; the kernel releases it if the
    /// process dies.
    pub fn acquire_online(home: &Path) -> Result<OnlineWriterFence> {
        let home = canonical_home(home)?;
        let file =
            sqlite_access::WriterLease::acquire(&home.join("writer.lock")).map_err(lease_error)?;
        Ok(OnlineWriterFence { _lock: file, home })
    }

    /// Acquires exclusive offline authority for init, migration, rollback,
    /// storage administration, deployment or installation. `exclusion` must
    /// have been taken for the same canonical home (else refused); then the
    /// writer lease is taken and, on an existing database, a quiescence proof
    /// runs: every recorded supervisor, startup owner and service process of
    /// an active or still-owning execution is strictly parsed (incomplete or
    /// malformed evidence refuses) and proven gone by absence, zombie state or
    /// a different identity at its pid (an inspection error refuses), and a
    /// bounded `BEGIN IMMEDIATE` probe succeeds. All locks stay held by the
    /// guard for the whole offline operation.
    pub fn acquire_exclusive(
        home: &Path,
        exclusion: BrokerExclusion,
    ) -> Result<OfflineWriterGuard> {
        let home = canonical_home(home)?;
        if exclusion.home != home {
            return Err(invalid("broker exclusion was taken for another home"));
        }
        let lock =
            sqlite_access::WriterLease::acquire(&home.join("writer.lock")).map_err(lease_error)?;
        if let Some(path) = state_path(&home)?
            && path.metadata()?.len() > 0
        {
            prove_quiescent(&path)?;
        }
        Ok(OfflineWriterGuard {
            _lock: lock,
            _exclusion: exclusion,
            home,
            store: None,
        })
    }
}

/// One recorded process identity that must be proven gone, compared with the
/// existing observation semantics: a Rust `linux:`/`darwin:` token compares
/// exactly, any other token only through its recorded legacy birth time, and
/// evidence without either is unknown.
struct Recorded {
    /// Recorded pid (> 1).
    pid: i32,
    /// Recorded identity token, when present.
    token: Option<String>,
    /// Recorded birth time, when present.
    birth: Option<f64>,
}

/// A pid usable as process evidence (2..=i32::MAX).
fn evidence_pid(pid: i64) -> Option<i32> {
    i32::try_from(pid).ok().filter(|pid| *pid > 1)
}

/// Parses a recorded owner: a JSON process identity (its birth, else
/// `birth_column`) or the legacy `"<pid> <token>"` text with its birth
/// column, exactly like startup reconciliation; anything else refuses.
fn parse_owner(raw: &str, birth_column: Option<f64>, what: &str) -> Result<Recorded> {
    let malformed = || invalid(format!("recorded {what} identity is malformed"));
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) {
        let pid = value["pid"]
            .as_i64()
            .and_then(evidence_pid)
            .ok_or_else(malformed)?;
        let token = value["token"].as_str().filter(|token| !token.is_empty());
        return Ok(Recorded {
            pid,
            token: Some(token.ok_or_else(malformed)?.to_owned()),
            birth: value["birth"].as_f64().or(birth_column),
        });
    }
    let (pid, _) = raw.split_once(' ').ok_or_else(malformed)?;
    let pid = pid
        .parse::<i64>()
        .ok()
        .and_then(evidence_pid)
        .ok_or_else(malformed)?;
    Ok(Recorded {
        pid,
        token: Some(raw.to_owned()),
        birth: Some(birth_column.ok_or_else(malformed)?),
    })
}

/// Column names of `table` (empty when the table is absent).
fn columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut statement = conn.prepare("SELECT name FROM pragma_table_info(?1)")?;
    let names = statement.query_map([table], |row| row.get(0))?;
    Ok(names.collect::<rusqlite::Result<_>>()?)
}

/// Collects the recorded identities of active or still-owning executions and
/// live service generations from the current or a supported historical
/// schema shape, failing closed on incomplete evidence or an unknown shape.
/// The supervisor and the startup owner are each checked independently.
fn recorded_identities(conn: &Connection) -> Result<Vec<Recorded>> {
    let tables: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    if tables == 0 {
        return Ok(Vec::new());
    }
    let agents = columns(conn, "agents")?;
    let has = |names: &[String], name: &str| names.iter().any(|column| column == name);
    if !["status", "supervisor_pid", "supervisor_identity"]
        .iter()
        .all(|name| has(&agents, name))
    {
        return Err(invalid(
            "unknown database shape; offline maintenance refuses",
        ));
    }
    let optional = |name: &str| {
        if has(&agents, name) {
            name.to_owned()
        } else {
            "NULL".into()
        }
    };
    let attempts = columns(conn, "attempts")?;
    let owning = if has(&attempts, "agent_id") && has(&attempts, "ownership_active") {
        " OR EXISTS(SELECT 1 FROM attempts t WHERE t.agent_id=a.id AND t.ownership_active=1)"
    } else {
        ""
    };
    let mut identities = Vec::new();
    let mut statement = conn.prepare(&format!(
        "SELECT supervisor_pid,supervisor_identity,{},{},{} FROM agents a WHERE a.status IN {ACTIVE_SQL}{owning}",
        optional("supervisor_birth_time"),
        optional("startup_owner_pid_identity"),
        optional("startup_owner_birth_time"),
    ))?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, Option<i64>>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<f64>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<f64>>(4)?,
        ))
    })?;
    for row in rows {
        let (pid, token, birth, startup, startup_birth) = row?;
        let token = token.filter(|token| !token.is_empty());
        match pid {
            Some(_) if token.is_none() && birth.is_none() => {
                return Err(invalid("recorded supervisor identity is incomplete"));
            }
            Some(pid) => identities.push(Recorded {
                pid: evidence_pid(pid)
                    .ok_or_else(|| invalid("recorded supervisor pid is invalid"))?,
                token,
                birth,
            }),
            None if token.is_some() || birth.is_some() => {
                return Err(invalid("recorded supervisor identity is incomplete"));
            }
            None => {}
        }
        if let Some(raw) = startup {
            identities.push(parse_owner(&raw, startup_birth, "startup owner")?);
        }
    }
    drop(statement);
    let services = columns(conn, "managed_service_generations")?;
    if services.is_empty() {
        return Ok(identities);
    }
    if !has(&services, "state") || !has(&services, "process_identity_json") {
        return Err(invalid(
            "unknown database shape; offline maintenance refuses",
        ));
    }
    let mut statement = conn.prepare(
        "SELECT process_identity_json FROM managed_service_generations WHERE state!='stopped' AND process_identity_json IS NOT NULL",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for raw in rows {
        identities.push(parse_owner(&raw?, None, "service")?);
    }
    Ok(identities)
}

/// Proves every recorded process gone and probes the write lock. Only `Dead`
/// or `Reused` observations prove absence; alive, unknown or denied evidence
/// refuses.
fn prove_quiescent(path: &Path) -> Result<()> {
    use agent_run_platform::process::{ProcessState, observe};
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    conn.busy_timeout(Duration::from_millis(100))?;
    for recorded in recorded_identities(&conn)? {
        match observe(
            Some(recorded.pid),
            recorded.token.as_deref(),
            recorded.birth,
        ) {
            ProcessState::Dead | ProcessState::Reused => {}
            ProcessState::Alive => {
                return Err(invalid(
                    "a recorded execution or service process is still alive; stop it before offline maintenance",
                ));
            }
            _ => {
                return Err(invalid(
                    "a recorded process could not be proven gone; offline maintenance refuses",
                ));
            }
        }
    }
    conn.execute_batch("BEGIN IMMEDIATE; ROLLBACK")
        .map_err(|_| invalid("another writer holds the database; offline maintenance refuses"))?;
    Ok(())
}

/// Proof that no broker serves the canonical home: exclusive flocks on the
/// bind locks of both legitimate broker sockets, `.api.sock.lock` and
/// `.writer.sock.lock` in that home (the locks a serving broker holds). Held
/// until dropped and valid only for the same home.
pub struct BrokerExclusion {
    /// Canonical home the locks belong to.
    home: PathBuf,
    /// Held bind locks.
    _locks: Vec<File>,
}

impl Drop for BrokerExclusion {
    /// Release the completed parent's flock explicitly before descriptor close.
    /// A concurrently forked child may retain the open description until exec;
    /// mere close would incorrectly keep this ended offline operation exclusive.
    fn drop(&mut self) {
        for file in &self._locks {
            let _ = fs2::FileExt::unlock(file);
        }
    }
}

impl BrokerExclusion {
    /// Takes both bind locks of the canonical `home` without blocking.
    /// `broker_sockets` must name exactly the home's legitimate broker
    /// sockets, `api.sock` and `writer.sock` (after canonicalizing each
    /// parent directory); an empty, incomplete, duplicate, foreign or
    /// cross-home list is refused before any lock is taken. A held lock yields
    /// `Error::Conflict` (locks already taken are released). The canonical
    /// home is recorded in the proof and re-checked by `acquire_exclusive`.
    pub fn acquire(home: &Path, broker_sockets: &[&Path]) -> Result<Self> {
        let home = canonical_home(home)?;
        let mut given = Vec::with_capacity(broker_sockets.len());
        for socket in broker_sockets {
            let parent = socket
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .ok_or_else(|| invalid("broker socket path must name its directory"))?;
            let name = socket
                .file_name()
                .ok_or_else(|| invalid("broker socket path must name a socket"))?;
            let normalized = canonical_home(parent)?.join(name);
            if given.contains(&normalized) {
                return Err(invalid("broker socket listed twice"));
            }
            given.push(normalized);
        }
        given.sort();
        let mut expected = vec![home.join("api.sock"), home.join("writer.sock")];
        expected.sort();
        if given != expected {
            return Err(invalid(
                "broker sockets must be exactly this home's api.sock and writer.sock",
            ));
        }
        // Own explicit unlock as soon as each lock succeeds, including unwind
        // when the second file is busy or invalid before construction completes.
        let mut exclusion = Self {
            home,
            _locks: Vec::with_capacity(2),
        };
        for name in [".api.sock.lock", ".writer.sock.lock"] {
            let file = lock_file(&exclusion.home, name)?;
            try_exclusive(&file)?;
            exclusion._locks.push(file);
        }
        Ok(exclusion)
    }
}

/// The held online writer lease; consumed by [`StoreWriter::open`].
pub struct OnlineWriterFence {
    /// Exclusively locked `writer.lock`.
    _lock: sqlite_access::WriterLease,
    /// Canonical home the lease protects.
    home: PathBuf,
}

/// Registers the mutation-fence function on an authorized connection.
pub(crate) fn register_fence(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        FENCE_FUNCTION,
        0,
        rusqlite::functions::FunctionFlags::SQLITE_UTF8
            | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |_| Ok(rusqlite::types::Null),
    )?;
    Ok(())
}

/// The sole online read-write store, owned by the writer thread.
pub struct StoreWriter<'connection> {
    /// Read-write store with the fence function registered.
    store: Store<'connection>,
    /// Held lease.
    _fence: std::sync::Arc<OnlineWriterFence>,
    /// Writer epoch of this incarnation.
    epoch: u64,
    /// Degradation entered by an uncertain outcome, until recovery.
    degraded: Option<crate::ops::Degradation>,
    /// One-shot fault switches of the `test-fixtures` feature.
    faults: Faults,
    /// Producers with unfinished scheduler work, captured by the scheduler
    /// when it claimed the next receipt-retention row; consumed by it.
    live_producers: Vec<String>,
}

/// One-shot fault switches injected by tests through the `test-fixtures`
/// feature; without the feature they never fire.
#[derive(Default)]
struct Faults {
    /// Fail the next maintenance settlement.
    #[cfg(feature = "test-fixtures")]
    settle: std::cell::Cell<bool>,
    /// Fail the next reservation read.
    #[cfg(feature = "test-fixtures")]
    reservations: std::cell::Cell<bool>,
}

impl Faults {
    /// Whether the next maintenance settlement must fail (consumes it).
    fn settle(&self) -> bool {
        #[cfg(feature = "test-fixtures")]
        {
            self.settle.replace(false)
        }
        #[cfg(not(feature = "test-fixtures"))]
        {
            false
        }
    }

    /// Whether the next reservation read must fail (consumes it).
    fn reservations(&self) -> bool {
        #[cfg(feature = "test-fixtures")]
        {
            self.reservations.replace(false)
        }
        #[cfg(not(feature = "test-fixtures"))]
        {
            false
        }
    }
}

impl StoreWriter<'_> {
    /// Opens the existing schema-30 database under the consumed lease. An
    /// absent, empty, older or newer database is refused with a typed
    /// unsupported error; this never initializes or migrates. Registers the
    /// fence function, verifies WAL/FULL/foreign keys and bumps the writer
    /// epoch with this process as holder.
    pub fn open(fence: OnlineWriterFence) -> Result<Self> {
        let home = fence.home.clone();
        // A symlinked or non-regular state.db is refused before any open, so
        // this lease can never mutate another home's database.
        let path = match state_path(&home)? {
            Some(path) if path.metadata()?.len() > 0 => path,
            _ => {
                return Err(Error::Unsupported(
                    "state database is missing; run offline init".into(),
                ));
            }
        };
        let conn = open_writer_connection(&path)?;
        let holder = agent_run_platform::process::inspect(std::process::id() as i32).ok();
        let epoch: i64 = {
            let tx = WriteTx::begin(&conn)?;
            tx.execute(
                "INSERT INTO writer_authority(id,epoch,holder_pid,holder_birth,holder_token,acquired_at) \
                 VALUES(1,1,?1,?2,?3,?4) ON CONFLICT(id) DO UPDATE SET epoch=epoch+1, \
                 holder_pid=excluded.holder_pid,holder_birth=excluded.holder_birth, \
                 holder_token=excluded.holder_token,acquired_at=excluded.acquired_at,probe_at=NULL",
                rusqlite::params![
                    std::process::id() as i64,
                    holder.as_ref().map(|identity| identity.birth),
                    holder.as_ref().map(|identity| identity.token.clone()),
                    now()
                ],
            )?;
            let epoch =
                tx.query_row("SELECT epoch FROM writer_authority WHERE id=1", [], |row| {
                    row.get(0)
                })?;
            tx.commit()?;
            epoch
        };
        Ok(Self {
            store: Store {
                conn: conn.into(),
                home,
                attempt: None,
            },
            _fence: std::sync::Arc::new(fence),
            epoch: epoch as u64,
            degraded: None,
            faults: Faults::default(),
            live_producers: Vec::new(),
        })
    }

    /// Writer epoch of this incarnation.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Lower the per-statement busy wait before adopting this writer connection.
    /// Increasing the production allowance or changing a borrowed connection is
    /// refused; bounded fixture checks can use a shorter allowance.
    pub fn set_busy_wait(&mut self, wait: Duration) -> Result<()> {
        if wait > WRITER_BUSY
            || matches!(&self.store.conn, sqlite_access::ConnectionRef::Borrowed(_))
        {
            return Err(invalid(
                "writer busy wait cannot increase or replace library policy",
            ));
        }
        self.store.conn.busy_timeout(wait)?;
        Ok(())
    }

    /// Applies one remote envelope as one top-level transaction with its
    /// receipt (see `ops::apply`). Rows without a store implementation yet are
    /// refused as typed `Unsupported` before any transaction, leaving no
    /// receipt. A degradation signal is remembered for [`Self::degraded`].
    pub fn apply(
        &mut self,
        authority: &WriteAuthority,
        envelope: &crate::ops::WriteEnvelope,
    ) -> SubmitResult {
        let kind = envelope.kind();
        if !crate::ops::implemented(kind) {
            return SubmitResult::Rejected(crate::ops::unsupported(kind));
        }
        let op = match envelope.operation() {
            Ok(op) => op,
            Err(error) => {
                return SubmitResult::Rejected(
                    agent_run_domain::storage::StorageRejection::from_error(
                        &error,
                        agent_run_domain::storage::RejectStage::Gate,
                    ),
                );
            }
        };
        let (execution, attempt, generation) = crate::ops::dispatch::remote_facts(&op);
        let checks = crate::ops::dispatch::remote_checks(&op);
        self.run(
            authority,
            envelope.header(),
            kind,
            (execution, attempt, generation),
            checks,
            |store| crate::ops::dispatch::run_remote(store, op),
        )
    }

    /// Applies one broker-local envelope. `authority` must be `Broker`, or
    /// `Worker` for rows 44-47 (the static matrix refuses anything else).
    pub fn apply_local(
        &mut self,
        authority: &WriteAuthority,
        envelope: &crate::ops::LocalEnvelope,
    ) -> SubmitResult {
        self.apply_local_owned(authority, envelope.clone())
    }

    /// [`Self::apply_local`] consuming the envelope, so its payload moves
    /// into the unit without a copy.
    pub fn apply_local_owned(
        &mut self,
        authority: &WriteAuthority,
        envelope: crate::ops::LocalEnvelope,
    ) -> SubmitResult {
        let kind = envelope.kind();
        if !crate::ops::implemented(kind) {
            return SubmitResult::Rejected(crate::ops::unsupported(kind));
        }
        let (header, op) = envelope.into_parts();
        let facts = crate::ops::dispatch::local_facts(&op);
        let live = std::mem::take(&mut self.live_producers);
        self.run(
            authority,
            &header,
            kind,
            facts,
            Default::default(),
            |store| {
                if kind != crate::ops::OpKindRef::PruneReceipts {
                    return crate::ops::dispatch::run_local(store, op);
                }
                // The live producers reach receipt retention through a
                // connection-private table filled and cleared inside this
                // write transaction.
                store.conn.execute_batch(
                    "CREATE TEMP TABLE IF NOT EXISTS writer_live_producers(key TEXT PRIMARY KEY); \
                     DELETE FROM temp.writer_live_producers;",
                )?;
                for key in &live {
                    store.conn.execute(
                        "INSERT OR IGNORE INTO temp.writer_live_producers(key) VALUES(?)",
                        [key],
                    )?;
                }
                let pruned = crate::ops::dispatch::run_local(store, op);
                store
                    .conn
                    .execute_batch("DELETE FROM temp.writer_live_producers;")?;
                pruned
            },
        )
    }

    /// Records the producers with unfinished scheduler work (queued, decoding,
    /// handed off or holding a withdrawn ordinal) for the next receipt
    /// retention row, which never prunes their watermarks. The scheduler
    /// captures them under its gate lock when it claims that row, so no
    /// admission can slip between capture and execution.
    pub fn set_live_producers(&mut self, producers: Vec<String>) {
        self.live_producers = producers;
    }

    /// Applies one remote envelope consuming it: a predecoded operation that
    /// the envelope holds alone moves into the unit without a copy, so at
    /// most the executing and the handed-off operation are decoded at once.
    pub fn apply_owned(
        &mut self,
        authority: &WriteAuthority,
        envelope: crate::ops::WriteEnvelope,
    ) -> SubmitResult {
        let kind = envelope.kind();
        if !crate::ops::implemented(kind) {
            return SubmitResult::Rejected(crate::ops::unsupported(kind));
        }
        let (header, op) = envelope.into_parts();
        let op = match op {
            Ok(op) => op,
            Err(error) => {
                return SubmitResult::Rejected(
                    agent_run_domain::storage::StorageRejection::from_error(
                        &error,
                        agent_run_domain::storage::RejectStage::Gate,
                    ),
                );
            }
        };
        let (execution, attempt, generation) = crate::ops::dispatch::remote_facts(&op);
        let checks = crate::ops::dispatch::remote_checks(&op);
        self.run(
            authority,
            &header,
            kind,
            (execution, attempt, generation),
            checks,
            |store| crate::ops::dispatch::run_remote(store, op),
        )
    }

    /// Stored outcome of `id` regardless of authority (trusted writer-thread
    /// read used to reconcile uncertain operations after recovery).
    pub fn stored_outcome(
        &self,
        id: &agent_run_domain::storage::OperationId,
    ) -> Result<Option<SubmitResult>> {
        crate::ops::apply::stored_outcome(&self.store.conn, id)
    }

    /// Makes the next maintenance settlement fail (test fixtures only).
    #[cfg(feature = "test-fixtures")]
    pub fn inject_settle_failure(&self) {
        self.faults.settle.set(true);
    }

    /// Makes the next reservation read fail (test fixtures only).
    #[cfg(feature = "test-fixtures")]
    pub fn inject_reservation_read_failure(&self) {
        self.faults.reservations.set(true);
    }

    /// Shared execution path of [`Self::apply`] and [`Self::apply_local`].
    fn run(
        &mut self,
        authority: &WriteAuthority,
        header: &crate::ops::EnvelopeHeader,
        kind: crate::ops::OpKindRef,
        facts: crate::ops::dispatch::Facts,
        checks: crate::ops::apply::Checks,
        run: impl FnOnce(&mut Store) -> Result<serde_json::Value>,
    ) -> SubmitResult {
        let (boot_id, now_ns) = match agent_run_platform::monotonic::now() {
            Ok(clock) => clock,
            Err(_) => {
                return SubmitResult::Rejected(agent_run_domain::storage::StorageRejection::new(
                    agent_run_domain::storage::RejectCode::StorageUnavailable,
                    agent_run_domain::storage::RejectStage::Gate,
                    "monotonic clock is unavailable",
                ));
            }
        };
        let (execution, attempt, generation) = facts;
        let executed = crate::ops::apply::execute(
            &mut self.store,
            authority,
            crate::ops::apply::Submission {
                header,
                kind,
                execution,
                attempt,
                generation,
                checks,
                boot_id: &boot_id,
                now_ns,
                epoch: self.epoch,
            },
            run,
        );
        if let Some(degradation) = executed.degraded {
            self.degraded = Some(degradation);
        }
        executed.result
    }

    /// The degradation entered by an earlier outcome, if any; cleared only by
    /// recovery.
    pub fn degraded(&self) -> Option<crate::ops::Degradation> {
        self.degraded
    }

    /// Stored outcome of `id` for its original `authority` (status path);
    /// `None` when absent or when the fingerprint differs. An unsettled writer
    /// transaction refuses: its visible rows cannot prove a durable commit.
    pub fn lookup(
        &self,
        id: &agent_run_domain::storage::OperationId,
        authority: &WriteAuthority,
    ) -> Result<Option<SubmitResult>> {
        if !self.store.conn.is_autocommit() {
            return Err(invalid(
                "writer receipt lookup requires a settled connection",
            ));
        }
        crate::ops::lookup(&self.store.conn, id, authority)
    }

    /// Resolve the exact original payload and authority without repeating a mutation.
    /// Missing or mismatched identity returns None, including durable rejections.
    pub fn lookup_digest(
        &self,
        id: &agent_run_domain::storage::OperationId,
        authority: &WriteAuthority,
        digest: &agent_run_domain::storage::PayloadDigest,
    ) -> Result<Option<SubmitResult>> {
        if !self.store.conn.is_autocommit() {
            return Err(invalid(
                "writer receipt lookup requires a settled connection",
            ));
        }
        crate::ops::apply::lookup_digest(&self.store.conn, id, authority, digest)
    }

    /// Runs one maintenance job between business transactions and outside
    /// any transaction, without receipt or replay. Refused while degraded or
    /// if a transaction is somehow open. A checkpoint reports SQLite's actual
    /// (busy, log, checkpointed) triple; an incremental vacuum is interrupted
    /// at `deadline` (never reported as success) and reports whether the
    /// free-page count decreased. Every maintenance statement's lock wait is
    /// bounded by the remaining deadline and the writer wait is restored.
    pub fn maintain(
        &mut self,
        op: crate::ops::MaintenanceOp,
        deadline: &MonoDeadline,
    ) -> MaintenanceOutcome {
        use agent_run_domain::storage::{RejectCode, RejectStage, StorageRejection};
        let refuse = |message: &str| {
            MaintenanceOutcome::Refused(StorageRejection::new(
                RejectCode::StorageUnavailable,
                RejectStage::Gate,
                message,
            ))
        };
        if self.degraded.is_some() || !self.store.conn.is_autocommit() {
            return refuse("maintenance is refused while the writer is degraded or busy");
        }
        let conn = &self.store.conn;
        let outcome = maintain_bounded(conn, op, deadline, &refuse);
        if !settled(conn) || self.faults.settle() {
            // The connection may still hold a transaction or a shortened
            // wait: no outcome is claimed and recovery reopens it.
            self.degraded = Some(crate::ops::Degradation::RollbackUncertain);
            return refuse("maintenance could not restore the writer connection");
        }
        outcome
    }

    /// Active admitted attempts (`ownership_active`, including terminal but
    /// still owned) and live service generations, for restoring and
    /// refreshing the scheduler's critical reservations.
    pub fn reservation_counts(&self) -> Result<(u64, u64)> {
        let attempts: i64 = self.store.conn.query_row(
            "SELECT COUNT(*) FROM attempts WHERE ownership_active=1",
            [],
            |row| row.get(0),
        )?;
        let services: i64 = self.store.conn.query_row(
            "SELECT COUNT(*) FROM managed_service_generations WHERE state<>'stopped'",
            [],
            |row| row.get(0),
        )?;
        Ok((attempts as u64, services as u64))
    }

    /// Reservation scopes from durable ownership: every ownership-active
    /// attempt with its execution (terminal but still owned included), every
    /// service generation not yet stopped, and every registered readiness
    /// probe with its generation (to scope rows naming only an attempt or a
    /// probe).
    pub fn reservation_scopes(&self) -> Result<ReservationScopes> {
        if self.faults.reservations() {
            return Err(Error::Runtime("injected reservation read failure".into()));
        }
        let conn = &self.store.conn;
        let mut statement = conn.prepare(
            "SELECT agent_id,id FROM attempts WHERE ownership_active=1 ORDER BY agent_id,id",
        )?;
        let attempts = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .map(|row| {
                let (execution, attempt) = row?;
                Ok((execution.parse()?, attempt))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut statement = conn.prepare(
            "SELECT id FROM managed_service_generations WHERE state<>'stopped' ORDER BY id",
        )?;
        let generations = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut statement =
            conn.prepare("SELECT id,generation_id FROM managed_service_probes ORDER BY id")?;
        let probes = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(ReservationScopes {
            attempts,
            generations,
            probes,
        })
    }

    /// Degraded recovery under the same lease: rolls back an open
    /// transaction (best effort) and closes the old connection before
    /// opening exactly one new fenced read-write connection (a failed close
    /// keeps the old one and refuses; a failed open leaves no read-write
    /// connection), then runs a private probe
    /// transaction. Success clears the degraded state; failure keeps it and
    /// returns the error. Uncertain operations are resolved afterwards by
    /// receipt lookup; a missing receipt stays unknown.
    pub fn recover(&mut self) -> Result<()> {
        if matches!(&self.store.conn, sqlite_access::ConnectionRef::Borrowed(_)) {
            return Err(Error::Unsupported(
                "library-owned writer recovery requires a verified connection restart".into(),
            ));
        }
        // The old connection is rolled back and closed before the new one
        // opens, so two read-write connections never coexist. Until the new
        // connection opens, an unfenced in-memory placeholder holds its place.
        let old = std::mem::replace(&mut self.store.conn, Connection::open_in_memory()?.into());
        let sqlite_access::ConnectionRef::Owned(old) = old else {
            unreachable!()
        };
        if !old.is_autocommit() {
            let _ = old.execute_batch("ROLLBACK");
        }
        if let Err((old, error)) = old.close() {
            self.store.conn = old.into();
            return Err(error.into());
        }
        let path = state_path(&self.store.home)?
            .ok_or_else(|| Error::Unsupported("state database is missing".into()))?;
        self.store.conn = open_writer_connection(&path)?.into();
        let probe = (|| -> Result<()> {
            let tx = WriteTx::begin(&self.store.conn)?;
            tx.execute("UPDATE writer_authority SET probe_at=? WHERE id=1", [now()])?;
            tx.commit()
        })();
        probe?;
        self.degraded = None;
        Ok(())
    }
}

/// Bounds the connection's lock wait by the time left until `deadline`
/// (at most [`WRITER_BUSY`]), read fresh for each statement; `Ok(false)`
/// when the deadline has passed.
fn bound_wait(conn: &Connection, deadline: &MonoDeadline) -> rusqlite::Result<bool> {
    match agent_run_platform::monotonic::now() {
        Ok((boot, now)) if boot == deadline.boot_id && now < deadline.ns => {
            conn.busy_timeout(Duration::from_nanos(deadline.ns - now).min(WRITER_BUSY))?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// Bounds the lock wait of a closing observation by the time left until
/// `deadline`, or to no wait once it has passed; the observation itself is
/// still made, so completed work is never reported as interrupted.
fn bound_observation(conn: &Connection, deadline: &MonoDeadline) -> rusqlite::Result<()> {
    let remaining = match agent_run_platform::monotonic::now() {
        Ok((boot, now)) if boot == deadline.boot_id && now < deadline.ns => {
            Duration::from_nanos(deadline.ns - now)
        }
        _ => Duration::ZERO,
    };
    conn.busy_timeout(remaining.min(WRITER_BUSY))
}

/// Ends maintenance on the writer connection: any open transaction is
/// rolled back and autocommit confirmed, and the writer wait is restored;
/// `false` when either cannot be confirmed.
fn settled(conn: &Connection) -> bool {
    let rolled_back =
        conn.is_autocommit() || (conn.execute_batch("ROLLBACK").is_ok() && conn.is_autocommit());
    let restored = conn.busy_timeout(WRITER_BUSY).is_ok();
    rolled_back && restored
}

/// Runs one maintenance job, bounding each statement's lock wait by the time
/// still left until the deadline: a checkpoint reports its (busy, log,
/// checkpointed) triple; an incremental vacuum is interrupted at the deadline
/// by a progress handler that reads the clock itself and reports whether the
/// free-page count decreased.
fn maintain_bounded(
    conn: &Connection,
    op: crate::ops::MaintenanceOp,
    deadline: &MonoDeadline,
    refuse: &dyn Fn(&str) -> MaintenanceOutcome,
) -> MaintenanceOutcome {
    /// Before every statement: a passed deadline interrupts, otherwise the
    /// wait is the fresh remaining time.
    macro_rules! bounded {
        () => {
            match bound_wait(conn, deadline) {
                Ok(true) => {}
                Ok(false) => return MaintenanceOutcome::Interrupted,
                Err(_) => return refuse("maintenance could not bound its wait"),
            }
        };
    }
    match op {
        crate::ops::MaintenanceOp::CheckpointWal { truncate } => {
            let sql = if truncate {
                "PRAGMA wal_checkpoint(TRUNCATE)"
            } else {
                "PRAGMA wal_checkpoint(PASSIVE)"
            };
            bounded!();
            match conn.query_row(sql, [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))) {
                Ok((busy, log, checkpointed)) => MaintenanceOutcome::Checkpoint {
                    busy,
                    log,
                    checkpointed,
                },
                Err(_) => refuse("checkpoint failed"),
            }
        }
        crate::ops::MaintenanceOp::VacuumIncremental { pages } => {
            let pages = pages.min(1024);
            let free = |conn: &Connection| {
                conn.pragma_query_value(None, "freelist_count", |row| row.get::<_, i64>(0))
            };
            bounded!();
            let Ok(before) = free(conn) else {
                return refuse("vacuum could not read the free list");
            };
            bounded!();
            let (boot, ns) = (deadline.boot_id.clone(), deadline.ns);
            let expired = move || {
                agent_run_platform::monotonic::now()
                    .map_or(true, |(now_boot, now)| now_boot != boot || now >= ns)
            };
            if conn.progress_handler(1000, Some(expired)).is_err() {
                return refuse("vacuum could not install its deadline");
            }
            let ran = (|| -> rusqlite::Result<()> {
                let mut statement = conn.prepare(&format!("PRAGMA incremental_vacuum({pages})"))?;
                let mut rows = statement.query([])?;
                while rows.next()?.is_some() {}
                Ok(())
            })();
            if conn.progress_handler(0, None::<fn() -> bool>).is_err() {
                return refuse("vacuum could not clear its deadline");
            }
            // A vacuum that ran is reported as such, never as interrupted: the
            // closing free-list read waits at most the time left (none once
            // the deadline passed).
            if ran.is_ok() && bound_observation(conn, deadline).is_err() {
                return refuse("vacuum could not bound its observation");
            }
            match ran {
                Ok(()) => match free(conn) {
                    Ok(after) => MaintenanceOutcome::Vacuumed {
                        reclaimed: after < before,
                    },
                    Err(_) => refuse("vacuum could not read the free list"),
                },
                Err(error)
                    if error.sqlite_error_code()
                        == Some(rusqlite::ErrorCode::OperationInterrupted) =>
                {
                    MaintenanceOutcome::Interrupted
                }
                Err(_) => refuse("vacuum failed"),
            }
        }
    }
}

/// Durable reservation scopes and the owner maps of rows that name only an
/// attempt or a probe.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReservationScopes {
    /// Ownership-active attempts as (execution, attempt).
    pub attempts: Vec<(agent_run_domain::domain::AgentId, String)>,
    /// Service generations not yet stopped.
    pub generations: Vec<String>,
    /// Registered readiness probes as (probe, generation).
    pub probes: Vec<(String, String)>,
}

/// Opens one fenced read-write connection to an existing schema-30 WAL
/// database without following symlinks: busy wait [`WRITER_BUSY`], foreign
/// keys, `synchronous=FULL` and the fence function.
fn open_writer_connection(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    conn.busy_timeout(WRITER_BUSY)?;
    let version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version != VERSION {
        return Err(Error::Unsupported(format!(
            "state schema v{version} is not v{VERSION}; run offline migration"
        )));
    }
    conn.pragma_update(None, "foreign_keys", true)?;
    let mode: String = conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(Error::Unsupported(
            "state database is not in WAL mode".into(),
        ));
    }
    conn.pragma_update(None, "synchronous", "FULL")?;
    register_fence(&conn)?;
    Ok(conn)
}

/// Offline exclusive authority; holds the writer lease and broker exclusion
/// for the whole offline operation.
pub struct OfflineWriterGuard {
    /// Held writer lease.
    _lock: sqlite_access::WriterLease,
    /// Held broker bind locks.
    _exclusion: BrokerExclusion,
    /// Home directory.
    home: std::path::PathBuf,
    /// Lazily opened read-write store.
    store: Option<Store<'static>>,
}

impl Drop for OfflineWriterGuard {
    /// Close offline SQL before releasing writer/broker exclusion. No callback
    /// can keep this synchronous connection alive after the guarded operation.
    fn drop(&mut self) {
        self.store.take();
    }
}

impl OfflineWriterGuard {
    /// Initializes an absent or empty database at schema 30.
    pub fn initialize(&mut self) -> Result<()> {
        let mut store = Store::connect(&self.home, true)?;
        register_fence(&store.conn)?;
        store.attempt = None;
        self.store = Some(store);
        Ok(())
    }

    /// Applies pending numbered migrations and returns the resulting version.
    pub fn migrate(&mut self) -> Result<i64> {
        crate::migrations::migrate(&self.home.join("state.db"))
    }

    /// The read-write store under this guard (opened on first use).
    pub fn store(&mut self) -> Result<&mut Store<'static>> {
        if self.store.is_none() {
            let store = Store::connect(&self.home, false)?;
            register_fence(&store.conn)?;
            self.store = Some(store);
        }
        Ok(self.store.as_mut().expect("opened"))
    }
}

/// A read-only store handle for every online process other than the writer.
pub struct StoreReader {
    /// Store over a read-only `query_only` connection.
    store: Store<'static>,
    /// Test hook run between a history batch's candidate and projection
    /// reads (`test-fixtures` only).
    #[cfg(feature = "test-fixtures")]
    history_pause: std::cell::RefCell<Option<Box<dyn Fn() + Send>>>,
}

impl StoreReader {
    /// Read-only superset preflight for expired history and auxiliary tables.
    /// Finite Unix seconds at least the history window are required. False lets
    /// maintenance skip SQL writes; the writer still rechecks every protected row.
    pub fn history_work_pending(&self, at: f64) -> Result<bool> {
        if !at.is_finite() || at < crate::retention::HISTORY_SECONDS {
            return Err(invalid("history retention requires a finite Unix time"));
        }
        self.store
            .prune_work_pending(at - crate::retention::HISTORY_SECONDS)
    }

    /// Consume this read-only facade without changing its connection mode.
    /// Compatibility SQL readers may retain the owned Store; SQLite READ_ONLY
    /// still refuses mutations, and this never creates or migrates a database.
    pub fn into_inner(self) -> Store<'static> {
        self.store
    }
    /// Opens with the ordinary five-second busy allowance.
    pub fn open(home: &Path) -> Result<Self> {
        Self::open_with(home, WRITER_BUSY)
    }

    /// Opens the existing schema-30 database read-only. Never creates,
    /// migrates or changes the file; a missing database or another schema
    /// version is a typed unsupported error. Writes through it fail with
    /// SQLITE_READONLY.
    pub fn open_with(home: &Path, busy: Duration) -> Result<Self> {
        let missing = || Error::Unsupported("state database is missing".into());
        let home = canonical_home(home).map_err(|_| missing())?;
        let path = state_path(&home)?.ok_or_else(missing)?;
        let conn = sqlite_access::reader_connection(&path, busy)?;
        let version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version != VERSION {
            return Err(Error::Unsupported(format!(
                "state schema v{version} is not v{VERSION}; offline migration required"
            )));
        }
        Ok(Self {
            store: Store {
                conn: conn.into(),
                home,
                attempt: None,
            },
            #[cfg(feature = "test-fixtures")]
            history_pause: Default::default(),
        })
    }

    /// Runs `hook` between the candidate and the projection reads of every
    /// later history batch (test fixtures only).
    #[cfg(feature = "test-fixtures")]
    pub fn pause_history_batch(&self, hook: impl Fn() + Send + 'static) {
        *self.history_pause.borrow_mut() = Some(Box::new(hook));
    }

    /// The read-only connection (for bounded raw reads).
    pub fn conn(&self) -> &Connection {
        &self.store.conn
    }

    /// Highest committed receipt sequence (0 when none).
    pub fn commit_seq(&self) -> Result<u64> {
        let seq: Option<i64> = self
            .store
            .conn
            .query_row(
                "SELECT MAX(commit_seq) FROM operation_receipts",
                [],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        Ok(seq.unwrap_or(0) as u64)
    }
}

impl Deref for StoreReader {
    type Target = Store<'static>;
    /// All read methods of the store over the read-only connection.
    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

/// Result of [`StoreReader::worker_catalog_preflight`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogPreflight {
    /// The identical proof is already registered for the authenticated attempt.
    AlreadyRegistered,
    /// Authenticated and not yet registered: submit the write.
    NeedsWrite,
    /// The capability or attempt did not authenticate.
    Unauthenticated,
    /// A different catalog is already registered.
    Conflict,
}

impl StoreReader {
    /// Read-only delivery preflight: due/reclaimable notices or expired unbound rows.
    /// This is only a hint; the writer rechecks eligibility atomically when claiming.
    /// Finite nonnegative Unix seconds are required. An empty schedule needs no write.
    pub fn delivery_pending(&self, at: f64) -> Result<bool> {
        if !at.is_finite() || at < 0.0 {
            return Err(invalid("invalid delivery preflight time"));
        }
        Ok(self.store.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM deliveries WHERE \
             (state IN ('pending','retry_wait') AND COALESCE(next_attempt_at,0)<=?1) \
             OR (state='sending' AND lease_until<=?1) \
             OR (state='waiting_binding' AND id NOT IN (SELECT completion_delivery_id FROM pools WHERE completion_delivery_id IS NOT NULL) \
                 AND agent_id IN (SELECT id FROM agents WHERE status IN ('succeeded','failed','timed_out','cancelled','lost')) \
                 AND terminal_event_seq IN (SELECT seq FROM events WHERE at<=?2)))",
            rusqlite::params![at, at - crate::delivery::BINDING_WINDOW_SECONDS],
            |row| row.get(0),
        )?)
    }

    /// Whether an eligible pending command exists for `run` (read-only F1
    /// preflight before submitting a claim). Cancels are always eligible;
    /// native-completion wakes only when `allow_native_wake`.
    pub fn command_pending(
        &self,
        run: &agent_run_domain::domain::AgentId,
        allow_native_wake: bool,
    ) -> Result<bool> {
        Ok(self.store.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM commands WHERE agent_id=? AND state='pending' AND (? OR kind='cancel' OR COALESCE(json_extract(payload_json,'$.native_completion'),0)<>1))",
            rusqlite::params![run.as_str(), allow_native_wake],
            |row| row.get(0),
        )?)
    }

    /// Kind and payload of a command already claimed (or completed) by `run`;
    /// `None` for an unknown id, another execution's command or a pending one.
    pub fn command_payload(
        &self,
        run: &agent_run_domain::domain::AgentId,
        command_id: i64,
    ) -> Result<Option<(String, serde_json::Value)>> {
        let row: Option<(String, String)> = self
            .store
            .conn
            .query_row(
                "SELECT kind,payload_json FROM commands WHERE id=? AND agent_id=? AND state IN ('claimed','completed')",
                rusqlite::params![command_id, run.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        row.map(|(kind, payload)| Ok((kind, serde_json::from_str(&payload)?)))
            .transpose()
    }

    /// Whether an explicit-finish turn transition for `run`'s owned, unfinished
    /// `attempt` would change its phase (read-only F1 preflight before
    /// submitting `WorkerTurn`). False when the phase already matches, finish
    /// was accepted, or the attempt is not the owned current one.
    pub fn worker_turn_pending(
        &self,
        run: &agent_run_domain::domain::AgentId,
        attempt: &str,
        idle: bool,
    ) -> Result<bool> {
        let phase = if idle { "idle" } else { "running" };
        Ok(self.store.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM worker_lifecycle l JOIN attempts t ON t.id=l.attempt_id \
             WHERE t.id=? AND t.agent_id=? AND t.ownership_active=1 AND t.finished_at IS NULL \
             AND l.phase<>? AND l.finish_json IS NULL)",
            rusqlite::params![attempt, run.as_str(), phase],
            |row| row.get(0),
        )?)
    }

    /// Token-authenticated read-only catalog preflight (F1) before
    /// submitting `RegisterWorkerCatalog`: unauthenticated proofs, identical
    /// registrations, different registrations and unregistered attempts are
    /// distinguished without any write.
    pub fn worker_catalog_preflight(
        &self,
        proof: &agent_run_domain::worker::WorkerCatalogProof,
        at: f64,
    ) -> Result<CatalogPreflight> {
        if crate::worker::authenticate_attempt(
            &self.store.conn,
            &proof.run_id,
            &proof.attempt_id,
            &proof.token,
            at,
        )?
        .is_none()
        {
            return Ok(CatalogPreflight::Unauthenticated);
        }
        let existing: (Option<u32>, Option<String>) = self.store.conn.query_row(
            "SELECT pool_catalog_version,pool_catalog_digest FROM worker_capabilities WHERE attempt_id=?",
            [&proof.attempt_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok(match existing {
            (None, None) => CatalogPreflight::NeedsWrite,
            (Some(version), Some(digest)) if version == proof.version && digest == proof.digest => {
                CatalogPreflight::AlreadyRegistered
            }
            _ => CatalogPreflight::Conflict,
        })
    }

    /// Normalized statistics projection computed on this reader (F3); the
    /// event scan never runs on the writer thread.
    pub fn run_stats_projection(
        &self,
        id: &agent_run_domain::domain::AgentId,
    ) -> Result<crate::ops::RunStatsRow> {
        crate::run_stats::project(&self.store.conn, id, now())
    }

    /// Bounded incident projection with its source proof computed on this
    /// reader from one consistent snapshot (F7); an unknown execution is
    /// `NotFound`.
    pub fn incident_projection(
        &self,
        id: &agent_run_domain::domain::AgentId,
        at: f64,
    ) -> Result<crate::ops::IncidentProjection> {
        let snapshot = self.store.conn.unchecked_transaction()?;
        crate::incidents::project(&snapshot, id.as_str(), at)?
            .ok_or_else(|| Error::NotFound(id.to_string()))
    }

    /// Read-only preflight: whether the execution's only owned provider
    /// attempt provably never spawned a child, so a terminal commit (row 42)
    /// would record its never-spawned proof. The writer rechecks inside the
    /// terminal transaction; this answer authorizes nothing.
    pub fn provider_never_spawned_pending(
        &self,
        id: &agent_run_domain::domain::AgentId,
    ) -> Result<bool> {
        Ok(self.store.conn.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM attempts t WHERE t.agent_id=? AND {})",
                crate::provider_admission::NEVER_SPAWNED
            ),
            [id.as_str()],
            |row| row.get(0),
        )?)
    }

    /// Prepares one history batch (row 85) for the sole writer without
    /// writing: up to `limit` (1..=2000) eligible candidates, oldest finish
    /// first, and their incident projections with source proofs observed at
    /// `at`, all read in one short read transaction under a cooperative
    /// two-second deadline, so candidates and projections share a snapshot.
    /// `at` is a finite Unix time of at least the history window. The batch
    /// is packed so its canonical encoding stays within row 85's decoded
    /// bound D: candidates that do not fit are left for a later batch, and
    /// a candidate whose projection cannot be bounded (or fit) is carried
    /// without one, which the writer defers. The writer rechecks every
    /// candidate and projection.
    pub fn history_batch(
        &self,
        at: f64,
        limit: usize,
    ) -> agent_run_domain::Result<crate::ops::PruneHistoryBatch> {
        self.history_page(at, limit, None).map(|(batch, _)| batch)
    }

    /// Prepare one bounded page and the last examined keyset position.
    /// Keep the returned cursor between maintenance calls so deferred prefixes
    /// cannot starve later candidates; None wraps an exhausted scan. Writer-side
    /// eligibility and complete durable evidence remain mandatory for deletion.
    /// A failed page yields no new cursor. Every read shares one short snapshot.
    pub fn history_page(
        &self,
        at: f64,
        limit: usize,
        after: Option<&crate::retention::HistoryCursor>,
    ) -> Result<(
        crate::ops::PruneHistoryBatch,
        Option<crate::retention::HistoryCursor>,
    )> {
        if !at.is_finite() || at < crate::retention::HISTORY_SECONDS {
            return Err(invalid("history retention requires a finite Unix time"));
        }
        if !(1..=2000).contains(&limit) {
            return Err(invalid("history batch limit must be 1..=2000"));
        }
        if after.is_some_and(|cursor| !cursor.finished_at.is_finite() || cursor.finished_at < 0.0) {
            return Err(invalid("invalid history scan cursor"));
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        self.store
            .conn
            .progress_handler(1_000, Some(move || Instant::now() >= deadline))?;
        let prepared = (|| -> Result<_> {
            let snapshot = self.store.conn.unchecked_transaction()?;
            let selected = crate::retention::history_candidates(&snapshot, at, limit, after)?;
            #[cfg(feature = "test-fixtures")]
            if let Some(hook) = self.history_pause.borrow().as_ref() {
                hook();
            }
            // ASCII-escaped canonical bytes bound the compact encoding too;
            // each element adds at most its bytes plus one separator.
            let encoded =
                |value: serde_json::Value| agent_run_domain::canonical::dumps(&value, true).len();
            let mut batch = crate::ops::PruneHistoryBatch {
                at,
                candidates: Vec::new(),
                projections: Vec::new(),
            };
            let budget = crate::ops::OpKindRef::PruneHistoryBatch.decoded_max();
            let mut used = encoded(serde_json::to_value(crate::ops::LocalOp::from(
                batch.clone(),
            ))?);
            let mut next = None;
            for cursor in selected {
                let id = cursor.execution.clone();
                if batch.candidates.len() >= limit {
                    break;
                }
                let projection = crate::incidents::project(&snapshot, id.as_str(), at)?;
                let cost = encoded(serde_json::to_value(&id)?) + 1;
                if used + cost > budget {
                    break;
                }
                used += cost;
                if let Some(projection) = projection
                    && crate::incidents::validate(&projection).is_ok()
                {
                    let cost = encoded(serde_json::to_value(&projection)?) + 1;
                    if used + cost <= budget {
                        used += cost;
                        batch.projections.push(projection);
                    }
                }
                batch.candidates.push(id);
                next = Some(cursor);
            }
            Ok((batch, next))
        })();
        let cleanup = self.store.conn.progress_handler(0, None::<fn() -> bool>);
        let prepared = prepared?;
        cleanup?;
        Ok(prepared)
    }

    /// Committed receipt of one operation identity, if present (read path for
    /// trusted online processes; status for a peer goes through
    /// [`Self::status_of`]). Durable rejections are not receipts here.
    pub fn receipt(
        &self,
        id: &agent_run_domain::storage::OperationId,
    ) -> Result<Option<agent_run_domain::storage::WriteReceipt>> {
        Ok(
            match crate::ops::apply::stored_outcome(&self.store.conn, id)? {
                Some(agent_run_domain::storage::SubmitResult::Committed(receipt)) => Some(receipt),
                _ => None,
            },
        )
    }

    /// Stored outcome (committed or durable rejection) of `id` answered only
    /// to the original `authority` fingerprint; `None` otherwise.
    pub fn status_of(
        &self,
        id: &agent_run_domain::storage::OperationId,
        authority: &WriteAuthority,
    ) -> Result<Option<SubmitResult>> {
        crate::ops::lookup(&self.store.conn, id, authority)
    }

    /// Stored outcome of `id` for its original `authority` and payload
    /// `digest` (committed or durable rejected); `None` otherwise.
    pub fn status_of_digest(
        &self,
        id: &agent_run_domain::storage::OperationId,
        authority: &WriteAuthority,
        digest: &agent_run_domain::storage::PayloadDigest,
    ) -> Result<Option<SubmitResult>> {
        crate::ops::apply::lookup_digest(&self.store.conn, id, authority, digest)
    }
}

#[cfg(test)]
mod tests {
    //! Maintenance wait bounding and connection settlement of the writer.
    use super::*;

    /// Neither receipt lookup API can assert durability through an open transaction; rollback restores truthful lookup.
    #[test]
    fn receipt_lookup_refuses_open_transaction_even_for_known_receipts() {
        let home = tempfile::tempdir().unwrap();
        crate::fixture::initialize(home.path()).unwrap();
        let mut writer =
            StoreWriter::open(WriterLease::acquire_online(home.path()).unwrap()).unwrap();
        let (boot, now) = agent_run_platform::monotonic::now().unwrap();
        let id = agent_run_domain::storage::OperationId::new(&boot, now);
        let me = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
        // SAFETY: geteuid reads this test process identity without side effects.
        let authority = WriteAuthority::Cli {
            // SAFETY: geteuid reads this test process's effective identity without side effects.
            uid: unsafe { libc::geteuid() },
            pid: me.pid,
            birth: me.birth,
            token: me.token,
        };
        let envelope = crate::ops::WriteEnvelope::new(
            id.clone(),
            agent_run_domain::storage::ProducerId {
                kind: agent_run_domain::storage::ProducerKind::Cli,
                scope: None,
                instance: "one".into(),
            },
            1,
            MonoDeadline {
                boot_id: boot,
                ns: now + 60_000_000_000,
            },
            crate::ops::RemoteOp::from(crate::ops::RegisterAccount {
                record: agent_run_domain::catalog::AccountRecord {
                    account_id: "fixture-account".parse().unwrap(),
                    auth_family: "openai".parse().unwrap(),
                    secret_ref: "native:codex".parse().unwrap(),
                    status: agent_run_domain::catalog::AccountStatus::Enabled,
                },
            }),
        )
        .unwrap();
        let result = writer.apply(&authority, &envelope);
        assert!(
            matches!(result, SubmitResult::Committed(_)),
            "fixture receipt must commit: {result:?}"
        );
        writer.store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(writer.lookup(&id, &authority).is_err());
        assert!(
            writer
                .lookup_digest(&id, &authority, &envelope.header().digest)
                .is_err()
        );
        writer.store.conn.execute_batch("ROLLBACK").unwrap();
        assert!(matches!(
            writer.lookup(&id, &authority).unwrap(),
            Some(SubmitResult::Committed(_))
        ));
        assert!(matches!(
            writer
                .lookup_digest(&id, &authority, &envelope.header().digest)
                .unwrap(),
            Some(SubmitResult::Committed(_))
        ));
    }

    /// A deadline in the future bounds the wait by the time left (never above
    /// the writer wait); a passed deadline or another boot refuses to bound.
    #[test]
    fn bound_wait_uses_the_fresh_remaining_time() {
        let conn = Connection::open_in_memory().unwrap();
        let (boot, now) = agent_run_platform::monotonic::now().unwrap();
        let wait = |conn: &Connection| -> i64 {
            conn.pragma_query_value(None, "busy_timeout", |row| row.get(0))
                .unwrap()
        };
        let near = MonoDeadline {
            boot_id: boot.clone(),
            ns: now + 50_000_000,
        };
        assert!(bound_wait(&conn, &near).unwrap());
        assert!(wait(&conn) <= 50);
        let far = MonoDeadline {
            boot_id: boot.clone(),
            ns: now + 10_000_000_000,
        };
        assert!(bound_wait(&conn, &far).unwrap());
        assert_eq!(wait(&conn), WRITER_BUSY.as_millis() as i64);
        let passed = MonoDeadline {
            boot_id: boot,
            ns: now.saturating_sub(1),
        };
        assert!(!bound_wait(&conn, &passed).unwrap());
        let foreign = MonoDeadline {
            boot_id: "another-boot".into(),
            ns: u64::MAX,
        };
        assert!(!bound_wait(&conn, &foreign).unwrap());
    }

    /// A rollback the connection refuses leaves the transaction open:
    /// settlement reports failure instead of a settled connection; once the
    /// refusal is lifted the same connection settles.
    #[test]
    fn settled_reports_an_unconfirmed_rollback() {
        use rusqlite::hooks::{AuthAction, Authorization, TransactionOperation};
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        conn.authorizer(Some(
            |context: rusqlite::hooks::AuthContext<'_>| match context.action {
                AuthAction::Transaction {
                    operation: TransactionOperation::Rollback,
                } => Authorization::Deny,
                _ => Authorization::Allow,
            },
        ))
        .unwrap();
        assert!(!settled(&conn));
        assert!(!conn.is_autocommit());
        conn.authorizer(None::<fn(rusqlite::hooks::AuthContext<'_>) -> Authorization>)
            .unwrap();
        assert!(settled(&conn));
        assert!(conn.is_autocommit());
    }

    /// A closing observation waits at most the time left, and not at all once
    /// the deadline has passed.
    #[test]
    fn observation_wait_is_fresh_or_zero() {
        let conn = Connection::open_in_memory().unwrap();
        let (boot, now) = agent_run_platform::monotonic::now().unwrap();
        let wait = |conn: &Connection| -> i64 {
            conn.pragma_query_value(None, "busy_timeout", |row| row.get(0))
                .unwrap()
        };
        bound_observation(
            &conn,
            &MonoDeadline {
                boot_id: boot.clone(),
                ns: now + 40_000_000,
            },
        )
        .unwrap();
        assert!(wait(&conn) <= 40);
        bound_observation(
            &conn,
            &MonoDeadline {
                boot_id: boot,
                ns: now.saturating_sub(1),
            },
        )
        .unwrap();
        assert_eq!(wait(&conn), 0);
    }

    /// Settlement rolls back an open transaction, confirms autocommit and
    /// restores the writer wait.
    #[test]
    fn settled_rolls_back_and_restores_the_writer_wait() {
        let conn = Connection::open_in_memory().unwrap();
        conn.busy_timeout(Duration::from_millis(5)).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(settled(&conn));
        assert!(conn.is_autocommit());
        let wait: i64 = conn
            .pragma_query_value(None, "busy_timeout", |row| row.get(0))
            .unwrap();
        assert_eq!(wait, WRITER_BUSY.as_millis() as i64);
    }
}
