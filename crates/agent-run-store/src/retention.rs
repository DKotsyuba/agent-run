//! Fourteen-day and bounded-session-count retention for completed work; active
//! ownership and retained lineage win over both age and count.

use crate::Store;
use agent_run_domain::{domain::AgentId, error::invalid, Error, Result};
use rusqlite::{params, ErrorCode, TransactionBehavior};
use serde_json::Value;
use std::time::{Duration, Instant};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

/// Completed history expires strictly after fourteen days, measured from `finished_at`.
pub const HISTORY_SECONDS: f64 = 14.0 * 24.0 * 3600.0;
/// Logical sessions (resume lineages sharing one `root_agent_id`) kept from
/// count expiry; older lineages become count-expired.
///
/// All logical sessions are ranked by the latest `created_at` admitted in the
/// lineage (descending, tie-break by root id) and the newest
/// [`HISTORY_SESSIONS`] survive count expiry. Age expiry is independent and
/// still applies to them: a lineage older than [`HISTORY_SECONDS`] expires on
/// schedule however few sessions exist. Protected work inside an expired
/// lineage can keep it stored, so the retained set may temporarily exceed
/// this bound.
pub const HISTORY_SESSIONS: i64 = 100;
/// Maximum rows removed from each large journal in one transaction.
const ROW_BATCH: i64 = 2_000;

/// Safe structured SQLite result-code detail extracted from one store error.
///
/// Only numeric result codes and static code names are captured. SQLite
/// message strings, SQL text and bound values can echo configuration and
/// transcript content, so they are deliberately excluded from maintenance
/// diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteCodes {
    /// Primary SQLite result code (`extended & 0xff`, e.g. `5` for busy).
    pub primary: i32,
    /// Stable name of the primary code as spelled by the SQLite binding.
    pub primary_name: String,
    /// Extended SQLite result code (e.g. `517` for a busy WAL snapshot).
    pub extended: i32,
    /// Static name of the extended code when it is one of the well-known
    /// busy/locked/interrupt spellings, else `None`.
    pub extended_name: Option<&'static str>,
}

/// Extracts safe SQLite result-code detail from `error`, when it carries a
/// SQLite failure. Every other error returns `None` and callers log their own
/// stable category instead.
pub fn sqlite_codes(error: &Error) -> Option<SqliteCodes> {
    let source = as_sqlite(error)?.sqlite_error()?;
    let extended = source.extended_code;
    Some(SqliteCodes {
        primary: extended & 0xff,
        primary_name: format!("{:?}", source.code),
        extended,
        extended_name: extended_name(extended),
    })
}

/// True when a maintenance failure is ordinary writer contention or an interruption.
///
/// Busy, locked and interrupted SQLite outcomes mean the pass must defer: the
/// work may still exist, so callers retry later instead of treating the pass
/// as complete or reporting a data-integrity failure. Every other error keeps
/// its normal error contract.
pub fn is_writer_contention(error: &Error) -> bool {
    matches!(
        as_sqlite(error).and_then(rusqlite::Error::sqlite_error_code),
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked | ErrorCode::OperationInterrupted)
    )
}

/// Returns the SQLite source error when the domain error carries one.
fn as_sqlite(error: &Error) -> Option<&rusqlite::Error> {
    match error {
        Error::Sql(source) => Some(source),
        _ => None,
    }
}

/// Names the extended busy/locked/interrupt spellings maintenance defers on.
fn extended_name(extended: i32) -> Option<&'static str> {
    match extended {
        5 => Some("SQLITE_BUSY"),
        6 => Some("SQLITE_LOCKED"),
        9 => Some("SQLITE_INTERRUPT"),
        261 => Some("SQLITE_BUSY_RECOVERY"),
        262 => Some("SQLITE_LOCKED_SHAREDCACHE"),
        517 => Some("SQLITE_BUSY_SNAPSHOT"),
        _ => None,
    }
}

/// Bounded in-memory proof of paths and run identities still referenced by retained rows.
pub struct StorageProtection {
    /// Agent and continuation identities which a filesystem pass must preserve.
    ids: HashSet<String>,
    /// Decoded, normalized and resolved paths, deduplicated across retained records.
    paths: HashSet<PathBuf>,
    /// Latest `created_at` of the [`HISTORY_SESSIONS`]-th newest logical
    /// session, when at least that many sessions were stored while this proof
    /// was collected; `None` below the cap, where no lineage is count-expired.
    count_boundary: Option<f64>,
}

impl StorageProtection {
    /// Returns one empty proof, for callers collecting evidence themselves.
    pub fn empty() -> Self {
        Self {
            ids: HashSet::new(),
            paths: HashSet::new(),
            count_boundary: None,
        }
    }

    /// Returns true when a retained row owns `id` or contains `path` or one of its descendants.
    pub fn retains(&self, id: &str, path: &Path) -> bool {
        self.ids.contains(id) || self.paths.iter().any(|retained| retained.starts_with(path))
    }

    /// Returns the count-expiry boundary captured with this proof.
    ///
    /// This is the latest `created_at` of the [`HISTORY_SESSIONS`]-th newest
    /// logical session at snapshot time, or `None` when fewer sessions were
    /// stored. A run tree whose canonical id encodes a creation at or after
    /// the boundary belongs to a lineage that ranks inside the protected set,
    /// so it can never be proved count-expired from this evidence; one below
    /// the boundary may be, once a fresh read proves no agents row owns it
    /// (see [`Store::run_admitted`]). The snapshot may predate later
    /// admissions, which only ever push the boundary newer, so a stale value
    /// never widens what this proof can authorize.
    pub fn count_boundary(&self) -> Option<f64> {
        self.count_boundary
    }

    /// Returns every decoded, normalized and resolved path this proof protects.
    ///
    /// Callers deciding whether a shared store object is still referenced walk
    /// this set; it is exactly the evidence the snapshot collected, never a
    /// guess about the current configuration.
    pub fn protected_paths(&self) -> impl Iterator<Item = &Path> {
        self.paths.iter().map(PathBuf::as_path)
    }

    /// Protects one currently loaded configuration path, including `file:` credential refs.
    pub fn protect_path(&mut self, value: &str) {
        self.add_path(value);
    }

    /// Adds a validated absolute path decoded from retained structured evidence.
    fn add_path(&mut self, value: &str) {
        let value = value.strip_prefix("file:").unwrap_or(value);
        let path = Path::new(value);
        if path.is_absolute() {
            if !self.paths.insert(path.to_owned()) {
                return;
            }
            let mut normalized = PathBuf::from("/");
            for part in path.components() {
                match part {
                    std::path::Component::Normal(name) => normalized.push(name),
                    std::path::Component::ParentDir => {
                        normalized.pop();
                    }
                    _ => {}
                }
            }
            self.paths.insert(normalized);
            if let Ok(resolved) = path.canonicalize() {
                self.paths.insert(resolved);
            }
        }
    }

    /// Walks one parsed JSON value so escaped Unicode and slashes are decoded before comparison.
    fn add_json(&mut self, value: &Value) {
        match value {
            Value::String(text) => {
                self.add_path(text);
                if text.parse::<AgentId>().is_ok() {
                    self.ids.insert(text.clone());
                }
            }
            Value::Array(values) => {
                for value in values {
                    self.add_json(value);
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    self.add_json(value);
                }
            }
            _ => {}
        }
    }
}

impl Store {
    /// Removes one bounded portion of expired history at finite Unix time `at`.
    ///
    /// Returns the number of deleted rows (zero means no eligible work remains).
    /// History expires by age — strictly fourteen days after `finished_at` —
    /// or by count: every logical session (one resume lineage per
    /// `root_agent_id`, counted once however many runs it holds) is ranked by
    /// the latest `created_at` admitted in its lineage, newest first with the
    /// root id as deterministic tie-breaker, and lineages ranked outside the
    /// newest [`HISTORY_SESSIONS`] become eligible however recent they are.
    /// All logical sessions are counted when ranking, and protected work can
    /// keep an expired lineage stored, so more than [`HISTORY_SESSIONS`]
    /// sessions may survive; safety never yields to the numerical cap.
    /// Up to 32 terminal leaf agents are considered; recent/active descendants,
    /// workflows, unreleased service leases, unresolved process ownership and
    /// in-flight deliveries retain their dependencies, as does a runtime home
    /// with a still-prepared storage-layout row (an unfinished relocation).
    /// Terminal agents with an unknown completion time stay as uncertain
    /// evidence. A retired multi-run lineage therefore drains tail-first —
    /// its newest row first — and never strands an ancestor behind a broken
    /// foreign key. Old queued notices expire
    /// with their run. Journals drain before their agent metadata is removed, so
    /// large histories make progress over multiple calls. Accounts, quota
    /// exhaustion latches, live services and files on disk are never removed.
    /// Every batch commits atomically or rolls back on error; no await is allowed.
    /// A read-only eligibility probe first skips the writer transaction entirely
    /// when nothing can expire yet, so an idle database never competes for the
    /// writer lock. A busy, locked or interrupted SQLite outcome returns the
    /// corresponding error (see [`is_writer_contention`]): callers must defer
    /// and retry rather than treat it as completion or a data-integrity failure.
    /// Callback setup/cleanup errors propagate; cleanup is attempted even if the batch fails.
    pub fn prune_history(&mut self, at: f64) -> Result<usize> {
        if !at.is_finite() || at < HISTORY_SECONDS {
            return Err(invalid("history retention requires a finite Unix time"));
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        self.conn
            .progress_handler(1_000, Some(move || Instant::now() >= deadline))?;
        let result = self.prune_history_batch(at);
        let cleanup = self.conn.progress_handler(0, None::<fn() -> bool>);
        let deleted = result?;
        cleanup?;
        Ok(deleted)
    }

    /// Captures retained path evidence without taking a database writer lock.
    ///
    /// The read is limited to 20,000 metadata rows, 64 MiB of selected values,
    /// and a cooperative two-second deadline shared by SQLite and row processing.
    /// It also computes the count-expiry boundary once (see
    /// [`StorageProtection::count_boundary`]) under the same deadline, so a
    /// filesystem pass never repeats a ranking scan per candidate tree.
    /// It excludes task bodies and transcript journals. Malformed, oversized or
    /// interrupted evidence returns an error; callers must retain all candidates.
    /// Runtime homes with a still-prepared storage-layout row are protected so
    /// no filesystem pass deletes a relocation target mid-flight; committed
    /// rows describe a mapping and add no path. Absolute paths named by
    /// not-conclusively-released managed-service definitions are protected too,
    /// because a live service may hold a shared-tree file or working directory
    /// open directly. The progress handler is cleared
    /// on either outcome. OS I/O is not preempted.
    pub fn storage_protection_snapshot(&self) -> Result<StorageProtection> {
        let deadline = Instant::now() + Duration::from_secs(2);
        self.conn
            .progress_handler(1_000, Some(move || Instant::now() >= deadline))?;
        let result = self.storage_protection_until(deadline);
        let cleanup = self.conn.progress_handler(0, None::<fn() -> bool>);
        let proof = result?;
        cleanup?;
        Ok(proof)
    }

    /// Builds the complete protection set or rejects it under the caller's SQL deadline.
    /// Every selected column, including non-JSON account references, consumes the budget.
    fn storage_protection_until(&self, deadline: Instant) -> Result<StorageProtection> {
        let mut proof = StorageProtection {
            ids: HashSet::new(),
            paths: HashSet::new(),
            count_boundary: None,
        };
        let mut bytes = 0usize;
        let mut visited = 0usize;
        // Inspect borrowed SQLite values before allocating owned Strings or parsing JSON.
        let mut check = |row: &rusqlite::Row<'_>| -> Result<()> {
            visited += 1;
            for column in 0..row.as_ref().column_count() {
                match row.get_ref(column)? {
                    rusqlite::types::ValueRef::Null => {}
                    rusqlite::types::ValueRef::Text(value) => {
                        bytes = bytes.saturating_add(value.len())
                    }
                    _ => return Err(invalid("invalid storage protection metadata")),
                }
            }
            if visited > 20_000 || bytes > 64 * 1024 * 1024 || Instant::now() >= deadline {
                return Err(invalid("storage protection evidence exceeds bound"));
            }
            Ok(())
        };
        let mut agents = self.conn.prepare("SELECT id,parent_agent_id,root_agent_id,answer_path,identity_json,workdir,json_extract(request_json,'$.read_roots') FROM agents")?;
        let mut rows = agents.query([])?;
        while let Some(row) = rows.next()? {
            check(row)?;
            let id: String = row.get(0)?;
            proof.ids.insert(id);
            for column in [1, 2] {
                if let Some(id) = row.get::<_, Option<String>>(column)? {
                    if id.parse::<AgentId>().is_ok() {
                        proof.ids.insert(id);
                    }
                }
            }
            for column in [3, 5] {
                if let Some(path) = row.get::<_, Option<String>>(column)? {
                    proof.add_path(&path);
                }
            }
            for column in [4, 6] {
                if let Some(raw) = row.get::<_, Option<String>>(column)? {
                    proof.add_json(&serde_json::from_str::<Value>(&raw)?);
                }
            }
        }
        let mut attempts = self.conn.prepare(
            "SELECT adapter_state_json,session_facts_json,cleanup_proof_json FROM attempts",
        )?;
        let mut rows = attempts.query([])?;
        while let Some(row) = rows.next()? {
            check(row)?;
            for column in 0..3 {
                if let Some(raw) = row.get::<_, Option<String>>(column)? {
                    proof.add_json(&serde_json::from_str::<Value>(&raw)?);
                }
            }
        }
        // Registered credentials remain protected even when their account is disabled
        // or the current configuration no longer mentions them.
        let mut accounts = self
            .conn
            .prepare("SELECT secret_ref FROM provider_accounts")?;
        let mut rows = accounts.query([])?;
        while let Some(row) = rows.next()? {
            check(row)?;
            let reference: String = row.get(0)?;
            proof.add_path(&reference);
        }
        // A still-prepared layout row means a relocation into that physical
        // runtime home is unfinished: its home and any backup the coordinator
        // must still prove stay protected until the row commits or is removed.
        // Committed rows describe a mapping and pin nothing here, so expired
        // homes keep ageing out normally.
        let mut pending = self
            .conn
            .prepare("SELECT runtime_home FROM runtime_storage_layouts WHERE state='prepared'")?;
        let mut rows = pending.query([])?;
        while let Some(row) = rows.next()? {
            check(row)?;
            let runtime_home: String = row.get(0)?;
            proof.protect_path(&runtime_home);
        }
        // A service generation that is not conclusively released — not
        // stopped, or still leased, or still probed — may hold open files or a
        // working directory anywhere its frozen definition names, including
        // inside a shared tree, so its recorded absolute paths stay protected.
        let mut services = self.conn.prepare(
            "SELECT definition_json FROM managed_service_generations g \
             WHERE g.state!='stopped' \
               OR EXISTS(SELECT 1 FROM managed_service_leases l \
                         WHERE l.generation_id=g.id AND l.released_at IS NULL) \
               OR EXISTS(SELECT 1 FROM managed_service_probes p WHERE p.generation_id=g.id)",
        )?;
        let mut rows = services.query([])?;
        while let Some(row) = rows.next()? {
            check(row)?;
            let raw: String = row.get(0)?;
            proof.add_json(&serde_json::from_str::<Value>(&raw)?);
        }
        // One aggregate under the same cooperative deadline replaces a
        // per-candidate scan: the count-expiry boundary is a property of the
        // whole snapshot, not of any one tree, so it is computed exactly once
        // here. At or above the cap the boundary is the newest-hundredth
        // session's latest admission; below the cap no lineage can rank
        // outside the protected set and the boundary stays `None`.
        proof.count_boundary = self.conn.query_row(
            "SELECT CASE WHEN (SELECT count(DISTINCT COALESCE(NULLIF(root_agent_id,''),id))
                          FROM agents) >= ?1
                        THEN (SELECT MIN(latest) FROM (
                                SELECT MAX(created_at) AS latest FROM agents
                                 GROUP BY COALESCE(NULLIF(root_agent_id,''),id)
                                 ORDER BY latest DESC LIMIT ?1)) END",
            [HISTORY_SESSIONS],
            |row| row.get::<_, Option<f64>>(0),
        )?;
        if Instant::now() >= deadline {
            return Err(invalid("storage protection deadline exceeded"));
        }
        Ok(proof)
    }

    /// Runs one read-only eligibility probe before any writer transaction.
    ///
    /// The query mirrors the batch's eligibility sources as supersets: every
    /// row the batch could consider matches at least one `EXISTS` here, and
    /// the batch's ownership, lineage and lease protections are applied later
    /// inside its own transaction. Count expiry contributes one cheap
    /// superset: with at most [`HISTORY_SESSIONS`] logical sessions no lineage
    /// can rank outside the newest set, while more sessions guarantee at
    /// least one count-expired candidate. `false` therefore proves the next
    /// batch would delete nothing, so callers skip taking the database
    /// writer lock at all; WAL readers never block a concurrent writer.
    fn prune_work_pending(&self, cutoff: f64) -> Result<bool> {
        let pending: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM workflow_runs
                WHERE status IN ('succeeded','failed','cancelled','lost') AND finished_at < ?1)
             OR EXISTS(SELECT 1 FROM agents
                WHERE status IN ('succeeded','failed','timed_out','cancelled','lost')
                  AND finished_at < ?1)
             OR EXISTS(SELECT 1 FROM capacity_samples WHERE observed_at < ?1)
             OR EXISTS(SELECT 1 FROM capacity_route_snapshots WHERE valid_until < ?1)
             OR EXISTS(SELECT 1 FROM managed_service_generations
                WHERE state='stopped' AND COALESCE(checked_at,created_at) < ?1)
             OR EXISTS(SELECT 1 FROM orchestrator_sessions WHERE last_seen_at < ?1)
             OR (SELECT count(DISTINCT COALESCE(NULLIF(root_agent_id,''),id))
                   FROM agents) > ?2",
            params![cutoff, HISTORY_SESSIONS],
            |row| row.get(0),
        )?;
        Ok(pending)
    }

    /// Reports whether one fresh indexed read shows an admitted agents row for
    /// `id`, for a run tree the caller has already observed on disk.
    ///
    /// This is the per-tree half of count-retirement evidence (the pass-wide
    /// boundary lives in [`StorageProtection::count_boundary`]): a single
    /// primary-key lookup on one consistent snapshot taken after the tree was
    /// seen. Run trees are only ever created after their agents row is
    /// durably committed, so `Ok(false)` while the tree exists proves no
    /// admission owns it — its row was pruned or never existed — while an
    /// admission the protection snapshot missed, including one in the same
    /// wall-clock second, an id generated before admission, or a rolled-back
    /// clock, is still found here. The durable row, never the id's
    /// second-resolution timestamp, is the authority. Callers must treat any
    /// error as "a row exists" (fail closed) and retry on the next pass.
    pub fn run_admitted(&self, id: &str) -> Result<bool> {
        let admitted: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM agents WHERE id=?1)",
            params![id],
            |row| row.get(0),
        )?;
        Ok(admitted)
    }

    /// Runs one short transaction under the caller's progress/lock deadlines.
    fn prune_history_batch(&mut self, at: f64) -> Result<usize> {
        let cutoff = at - HISTORY_SECONDS;
        // An idle database must not take the writer lock at all: the probe
        // above is read-only, so an ordinary workload never sees a no-op
        // maintenance transaction compete for the single WAL writer.
        if !self.prune_work_pending(cutoff)? {
            return Ok(0);
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS retention_agents(id TEXT PRIMARY KEY);
             CREATE TEMP TABLE IF NOT EXISTS retention_workflows(id TEXT PRIMARY KEY);
             CREATE TEMP TABLE IF NOT EXISTS retention_services(id TEXT PRIMARY KEY);
             CREATE TEMP TABLE IF NOT EXISTS retention_sessions(id TEXT PRIMARY KEY);
             CREATE TEMP TABLE IF NOT EXISTS retention_roots(root TEXT PRIMARY KEY);
             DELETE FROM retention_agents; DELETE FROM retention_workflows;
             DELETE FROM retention_services; DELETE FROM retention_sessions;
             DELETE FROM retention_roots;",
        )?;
        // Count expiry: rank every logical session (one resume lineage per
        // root agent id, with unrooted legacy rows counting as their own
        // session) by the latest `created_at` admitted in the lineage,
        // newest first with the root id as the deterministic tie-breaker, and
        // keep everything ranked outside the newest HISTORY_SESSIONS. Only
        // whole sessions are selected, so a retired multi-run lineage drains
        // tail-first and its shrinking `created_at` maximum can never lift it
        // back into the protected set mid-drain.
        tx.execute(
            "INSERT INTO retention_roots
             SELECT session FROM (
               SELECT COALESCE(NULLIF(root_agent_id,''),id) AS session, MAX(created_at) AS latest
                 FROM agents GROUP BY session
                ORDER BY latest DESC, session LIMIT -1 OFFSET ?)",
            [HISTORY_SESSIONS],
        )?;
        tx.execute(
            "INSERT INTO retention_workflows
             SELECT w.id FROM workflow_runs w
             WHERE w.status IN ('succeeded','failed','cancelled','lost') AND w.finished_at < ?1
             AND NOT EXISTS (SELECT 1 FROM workflow_deliveries d WHERE d.run_id=w.id
               AND d.state='sending' AND (d.lease_until IS NULL OR d.lease_until > ?2))
             AND NOT EXISTS (SELECT 1 FROM workflow_steps s JOIN agents a ON a.id=s.agent_id
               WHERE s.run_id=w.id AND (a.status IN ('created','starting','running','cancelling')
                 OR a.finished_at IS NULL OR a.finished_at >= ?1
                 OR EXISTS (SELECT 1 FROM attempts t WHERE t.agent_id=a.id AND t.ownership_active=1)))
             LIMIT 32",
            params![cutoff, at],
        )?;
        let mut deleted = 0;
        for table in ["workflow_deliveries", "workflow_steps"] {
            deleted += tx.execute(
                &format!("DELETE FROM {table} WHERE run_id IN retention_workflows"),
                [],
            )?;
        }
        deleted += tx.execute(
            "DELETE FROM workflow_runs WHERE id IN retention_workflows",
            [],
        )?;
        tx.execute(
            "INSERT INTO retention_agents SELECT a.id FROM agents a
             WHERE a.status IN ('succeeded','failed','timed_out','cancelled','lost')
             AND (a.finished_at < ?1
                  OR (a.finished_at IS NOT NULL
                      AND COALESCE(NULLIF(a.root_agent_id,''),a.id) IN retention_roots))
             AND NOT EXISTS (SELECT 1 FROM attempts t WHERE t.agent_id=a.id AND t.ownership_active=1)
             AND NOT EXISTS (SELECT 1 FROM agents child WHERE child.parent_agent_id=a.id)
             AND NOT EXISTS (SELECT 1 FROM workflow_steps s WHERE s.agent_id=a.id)
             AND NOT EXISTS (SELECT 1 FROM managed_service_leases l WHERE l.agent_id=a.id AND l.released_at IS NULL)
             AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.agent_id=a.id AND d.state='sending'
               AND (d.lease_until IS NULL OR d.lease_until > ?2))
             AND NOT EXISTS (SELECT 1 FROM runtime_storage_layouts r
               WHERE r.state='prepared'
                 AND r.runtime_home=json_extract(a.identity_json,'$.runtime_home'))
             ORDER BY a.finished_at,a.id LIMIT 32",
            params![cutoff, at],
        )?;
        deleted += tx.execute(
            "DELETE FROM delivery_attempt_evidence WHERE rowid IN
             (SELECT e.rowid FROM delivery_attempt_evidence e JOIN deliveries d ON d.id=e.delivery_id
              WHERE d.agent_id IN retention_agents LIMIT ?)", [ROW_BATCH],
        )?;
        deleted += tx.execute(
            "DELETE FROM deliveries WHERE agent_id IN retention_agents
             AND NOT EXISTS (SELECT 1 FROM delivery_attempt_evidence e WHERE e.delivery_id=deliveries.id)", [],
        )?;
        for table in ["messages", "commands", "events"] {
            // A terminal event stays while a delivery still references it.
            let protected = if table == "events" {
                "AND seq NOT IN (SELECT terminal_event_seq FROM deliveries WHERE terminal_event_seq IS NOT NULL)"
            } else {
                ""
            };
            deleted += tx.execute(
                &format!(
                    "DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table}
                 WHERE agent_id IN retention_agents {protected} LIMIT ?)"
                ),
                [ROW_BATCH],
            )?;
        }
        // Remove only fully drained agents; retention never leaves broken foreign keys.
        tx.execute_batch(
            "DELETE FROM retention_agents WHERE
               EXISTS (SELECT 1 FROM events WHERE agent_id=retention_agents.id)
               OR EXISTS (SELECT 1 FROM messages WHERE agent_id=retention_agents.id)
               OR EXISTS (SELECT 1 FROM commands WHERE agent_id=retention_agents.id)
               OR EXISTS (SELECT 1 FROM deliveries WHERE agent_id=retention_agents.id);",
        )?;
        for table in ["process_members", "process_ownership"] {
            deleted += tx.execute(
                &format!(
                    "DELETE FROM {table} WHERE owner_kind='attempt'
                AND owner_id IN (SELECT id FROM attempts WHERE agent_id IN retention_agents)"
                ),
                [],
            )?;
        }
        deleted += tx.execute(
            "DELETE FROM attempt_quota_keys WHERE attempt_id IN
            (SELECT id FROM attempts WHERE agent_id IN retention_agents)",
            [],
        )?;
        for table in [
            "attempts",
            "run_stats",
            "agent_service_gates",
            "managed_service_leases",
        ] {
            deleted += tx.execute(
                &format!("DELETE FROM {table} WHERE agent_id IN retention_agents"),
                [],
            )?;
        }
        deleted += tx.execute("DELETE FROM agents WHERE id IN retention_agents", [])?;
        deleted += tx.execute(
            "DELETE FROM capacity_samples WHERE id IN
            (SELECT id FROM capacity_samples WHERE observed_at < ? LIMIT ?)",
            params![cutoff, ROW_BATCH],
        )?;
        deleted += tx.execute(
            "DELETE FROM capacity_route_snapshots WHERE valid_until < ?",
            [cutoff],
        )?;
        tx.execute(
            "INSERT INTO retention_services SELECT g.id FROM managed_service_generations g
            WHERE g.state='stopped' AND COALESCE(g.checked_at,g.created_at) < ?
            AND NOT EXISTS (SELECT 1 FROM managed_service_leases l WHERE l.generation_id=g.id)
            AND NOT EXISTS (SELECT 1 FROM managed_service_probes p WHERE p.generation_id=g.id)
            LIMIT 32",
            [cutoff],
        )?;
        for table in ["process_members", "process_ownership"] {
            deleted += tx.execute(
                &format!(
                    "DELETE FROM {table} WHERE owner_kind='service'
                AND owner_id IN retention_services"
                ),
                [],
            )?;
        }
        deleted += tx.execute(
            "DELETE FROM managed_service_generations WHERE id IN retention_services",
            [],
        )?;
        tx.execute("INSERT INTO retention_sessions SELECT s.id FROM orchestrator_sessions s
            WHERE s.last_seen_at < ?
            AND NOT EXISTS (SELECT 1 FROM agents a WHERE a.orchestrator_session_id=s.id)
            AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.orchestrator_session_id=s.id)
            AND NOT EXISTS (SELECT 1 FROM workflow_runs w WHERE w.orchestrator_session_id=s.id)
            AND NOT EXISTS (SELECT 1 FROM workflow_deliveries d WHERE d.orchestrator_session_id=s.id)
            LIMIT 32", [cutoff])?;
        deleted += tx.execute(
            "DELETE FROM context_receipts WHERE orchestrator_session_id IN retention_sessions",
            [],
        )?;
        deleted += tx.execute(
            "DELETE FROM orchestrator_sessions WHERE id IN retention_sessions",
            [],
        )?;
        tx.commit()?;
        Ok(deleted)
    }

    /// Reclaims up to 1,024 free SQLite pages without rebuilding the database.
    ///
    /// Returns true when pages were reclaimed (call again to drain the backlog). Active agents,
    /// workflow runs and live services defer compaction. Unresolved terminal
    /// ownership remains stored but does not block repacking existing pages. SQLite's
    /// progress callback interrupts work after three seconds; callers should retry
    /// later on contention/interruption. This is cooperative, not a hard I/O deadline.
    /// Schema 19 prepares incremental mode during offline migration. This method
    /// never replaces the database file or changes schema/auto-vacuum mode.
    /// Callback setup/cleanup errors propagate; cleanup is attempted even if vacuum fails.
    pub fn vacuum_history(&self) -> Result<bool> {
        let active: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM agents WHERE status IN ('created','starting','running','cancelling'))
             OR EXISTS(SELECT 1 FROM workflow_runs WHERE status IN ('created','running'))
             OR EXISTS(SELECT 1 FROM managed_service_generations WHERE state != 'stopped')",
            [], |r| r.get(0),
        )?;
        if active {
            return Ok(false);
        }
        // Also retries a checkpoint deferred by readers after an earlier batch.
        self.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        let free: i64 = self
            .conn
            .pragma_query_value(None, "freelist_count", |r| r.get(0))?;
        if free == 0 {
            return Ok(false);
        }
        let mode: i64 = self
            .conn
            .pragma_query_value(None, "auto_vacuum", |r| r.get(0))?;
        if mode != 2 {
            return Err(invalid(
                "history reclamation requires incremental vacuum mode",
            ));
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        self.conn
            .progress_handler(1_000, Some(move || Instant::now() >= deadline))?;
        let vacuum = (|| -> rusqlite::Result<()> {
            let mut statement = self.conn.prepare("PRAGMA incremental_vacuum(1024)")?;
            let mut rows = statement.query([])?;
            // SQLite yields after each reclaimed page. Drive the whole bounded batch.
            while rows.next()?.is_some() {}
            Ok(())
        })();
        let cleanup = self.conn.progress_handler(0, None::<fn() -> bool>);
        vacuum?;
        cleanup?;
        // A busy reader may defer truncation; a later idle pass checkpoints again.
        self.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        let after: i64 = self
            .conn
            .pragma_query_value(None, "freelist_count", |r| r.get(0))?;
        Ok(after < free)
    }
}
