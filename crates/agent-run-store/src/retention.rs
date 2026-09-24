//! Fourteen-day retention for completed work; active ownership and retained lineage win over age.

use crate::Store;
use agent_run_domain::{error::invalid, Result};
use rusqlite::{params, TransactionBehavior};
use std::time::{Duration, Instant};

/// Completed history expires strictly after fourteen days, measured from `finished_at`.
pub const HISTORY_SECONDS: f64 = 14.0 * 24.0 * 3600.0;
/// Maximum rows removed from each large journal in one transaction.
const ROW_BATCH: i64 = 2_000;

impl Store {
    /// Removes one bounded portion of expired history at finite Unix time `at`.
    ///
    /// Returns the number of deleted rows (zero means no eligible work remains).
    /// Up to 32 terminal leaf agents are considered; recent/active descendants,
    /// workflows, unreleased service leases, unresolved process ownership and
    /// in-flight deliveries retain their dependencies. Old queued notices expire
    /// with their run. Journals drain before their agent metadata is removed, so
    /// large histories make progress over multiple calls. Accounts, quota
    /// exhaustion latches, live services and files on disk are never removed.
    /// Every batch commits atomically or rolls back on error; no await is allowed.
    pub fn prune_history(&mut self, at: f64) -> Result<usize> {
        if !at.is_finite() || at < HISTORY_SECONDS {
            return Err(invalid("history retention requires a finite Unix time"));
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        self.conn
            .progress_handler(1_000, Some(move || Instant::now() >= deadline));
        let result = self.prune_history_batch(at);
        self.conn.progress_handler(0, None::<fn() -> bool>);
        result
    }

    /// Runs one short transaction under the caller's progress/lock deadlines.
    fn prune_history_batch(&mut self, at: f64) -> Result<usize> {
        let cutoff = at - HISTORY_SECONDS;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS retention_agents(id TEXT PRIMARY KEY);
             CREATE TEMP TABLE IF NOT EXISTS retention_workflows(id TEXT PRIMARY KEY);
             CREATE TEMP TABLE IF NOT EXISTS retention_services(id TEXT PRIMARY KEY);
             CREATE TEMP TABLE IF NOT EXISTS retention_sessions(id TEXT PRIMARY KEY);
             DELETE FROM retention_agents; DELETE FROM retention_workflows;
             DELETE FROM retention_services; DELETE FROM retention_sessions;",
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
             WHERE a.status IN ('succeeded','failed','timed_out','cancelled','lost') AND a.finished_at < ?1
             AND NOT EXISTS (SELECT 1 FROM attempts t WHERE t.agent_id=a.id AND t.ownership_active=1)
             AND NOT EXISTS (SELECT 1 FROM agents child WHERE child.parent_agent_id=a.id)
             AND NOT EXISTS (SELECT 1 FROM workflow_steps s WHERE s.agent_id=a.id)
             AND NOT EXISTS (SELECT 1 FROM managed_service_leases l WHERE l.agent_id=a.id AND l.released_at IS NULL)
             AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.agent_id=a.id AND d.state='sending'
               AND (d.lease_until IS NULL OR d.lease_until > ?2))
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
            .progress_handler(1_000, Some(move || Instant::now() >= deadline));
        let vacuum = (|| -> rusqlite::Result<()> {
            let mut statement = self.conn.prepare("PRAGMA incremental_vacuum(1024)")?;
            let mut rows = statement.query([])?;
            // SQLite yields after each reclaimed page. Drive the whole bounded batch.
            while rows.next()?.is_some() {}
            Ok(())
        })();
        self.conn.progress_handler(0, None::<fn() -> bool>);
        vacuum?;
        // A busy reader may defer truncation; a later idle pass checkpoints again.
        self.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        let after: i64 = self
            .conn
            .pragma_query_value(None, "freelist_count", |r| r.get(0))?;
        Ok(after < free)
    }
}
