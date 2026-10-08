//! Capability-bound worker reports sharing the existing delivery outbox.

use crate::Store;
use agent_run_domain::{
    Error, Result,
    domain::AgentId,
    worker::{NotifyReceipt, NotifyRequest},
};
use agent_run_platform::fs::sha256;
use rusqlite::{OptionalExtension, TransactionBehavior, params};

/// Maximum distinct reports accepted for one exact run.
const MAX_MESSAGES: i64 = 20;
/// Minimum seconds between accepted distinct reports for one exact run.
const MIN_INTERVAL: f64 = 30.0;

/// Validates the supervisor-generated 32-byte lowercase-hex bearer secret.
fn valid_token(token: &str) -> bool {
    token.len() == 64
        && token
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// One authenticated attempt with the durable facts its callers check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptAuth {
    /// Stable lineage root of the authenticated execution.
    pub root_agent_id: AgentId,
    /// Orchestrator session binding, when one exists.
    pub orchestrator_session_id: Option<String>,
    /// Whether the attempt is in the `running` state right now.
    pub running: bool,
}

/// Authenticates the exact live attempt behind a worker capability.
///
/// The token hash must match `worker_capabilities`, the attempt must be
/// ownership-active and unfinished, its agent live within its deadline, and
/// the optional orchestrator session is joined without requiring it. `None`
/// means the credentials or liveness failed; callers add only their own
/// predicates on top.
pub fn authenticate_attempt(
    conn: &rusqlite::Connection,
    run_id: &AgentId,
    attempt_id: &str,
    token: &str,
    at: f64,
) -> Result<Option<AttemptAuth>> {
    if !valid_token(token) || !at.is_finite() || at < 0.0 {
        return Ok(None);
    }
    let row: Option<(String, Option<String>, bool)> = conn
        .query_row(
            "SELECT c.token_sha256,a.orchestrator_session_id,t.state='running' AND a.status='running' \
             FROM worker_capabilities c \
             JOIN attempts t ON t.id=c.attempt_id JOIN agents a ON a.id=t.agent_id \
             LEFT JOIN orchestrator_sessions s ON s.id=a.orchestrator_session_id \
             WHERE t.id=? AND t.agent_id=? AND t.ownership_active=1 AND t.finished_at IS NULL \
               AND a.finished_at IS NULL AND a.created_at+a.timeout_seconds>?",
            rusqlite::params![attempt_id, run_id.as_str(), at],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((digest, session, running)) = row else {
        return Ok(None);
    };
    if digest != sha256(token.as_bytes()) {
        return Ok(None);
    }
    Ok(Some(AttemptAuth {
        root_agent_id: resolve_root(conn, run_id)?,
        orchestrator_session_id: session,
        running,
    }))
}

/// Reads the lineage root of an existing execution row.
fn resolve_root(conn: &rusqlite::Connection, run_id: &AgentId) -> Result<AgentId> {
    let root: String = conn.query_row(
        "SELECT CASE WHEN root_agent_id='' THEN id ELSE root_agent_id END FROM agents WHERE id=?",
        [run_id.as_str()],
        |row| row.get(0),
    )?;
    root.parse()
}

impl Store {
    /// Binds one ephemeral secret hash to a currently owned attempt before launch.
    /// Repeating the same binding is harmless; a replacement secret is refused.
    pub fn issue_worker_capability(
        &mut self,
        run_id: &AgentId,
        attempt_id: &str,
        token: &str,
        at: f64,
    ) -> Result<()> {
        if !valid_token(token) || !at.is_finite() || at < 0.0 {
            return Err(Error::Validation("invalid worker capability".into()));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let owned: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE id=? AND agent_id=? AND ownership_active=1 AND finished_at IS NULL)",
            params![attempt_id, run_id.as_str()],
            |row| row.get(0),
        )?;
        if !owned {
            return Err(Error::Validation("worker attempt is not active".into()));
        }
        let digest = sha256(token.as_bytes());
        let existing: Option<String> = tx
            .query_row(
                "SELECT token_sha256 FROM worker_capabilities WHERE attempt_id=?",
                [attempt_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing != digest {
                return Err(Error::Conflict);
            }
        } else {
            tx.execute(
                "INSERT INTO worker_capabilities(attempt_id,token_sha256,created_at) VALUES(?,?,?)",
                params![attempt_id, digest, at],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Registers proof from the real native worker MCP, including legitimate
    /// owned STARTING attempts. Unknown/old catalogs remain unsupported; no
    /// token value is persisted. Repeated identical proof is an idempotent read.
    pub fn register_worker_catalog(
        &mut self,
        proof: &agent_run_domain::worker::WorkerCatalogProof,
        at: f64,
    ) -> Result<()> {
        if !agent_run_domain::worker::known_pool_catalog(proof.version, &proof.digest) {
            return Err(Error::Unsupported(
                "worker pool catalog is not supported".into(),
            ));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if authenticate_attempt(&tx, &proof.run_id, &proof.attempt_id, &proof.token, at)?.is_none()
        {
            return Err(Error::Validation("invalid worker capability".into()));
        }
        let existing:(Option<u32>,Option<String>)=tx.query_row(
            "SELECT pool_catalog_version,pool_catalog_digest FROM worker_capabilities WHERE attempt_id=?",
            [&proof.attempt_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
        if existing.0.is_some() || existing.1.is_some() {
            if existing.0 != Some(proof.version)
                || existing.1.as_deref() != Some(proof.digest.as_str())
            {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(());
        }
        tx.execute(
            "UPDATE worker_capabilities SET pool_catalog_version=?,pool_catalog_digest=?,pool_catalog_observed_at=? WHERE attempt_id=?",
            params![proof.version, proof.digest, at, proof.attempt_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Authenticates the exact running attempt and atomically queues one report.
    /// Idempotent replays bypass volume limits but never bypass authentication.
    pub fn notify_orchestrator(
        &mut self,
        run_id: &AgentId,
        attempt_id: &str,
        token: &str,
        input: &NotifyRequest,
        at: f64,
    ) -> Result<NotifyReceipt> {
        input.validate()?;
        if !valid_token(token) || !at.is_finite() || at < 0.0 || input.message.contains(token) {
            return Err(Error::Validation("invalid worker report".into()));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let auth = authenticate_attempt(&tx, run_id, attempt_id, token, at)?
            .filter(|auth| auth.running)
            .and_then(|auth| {
                // The report route additionally requires the exact historical
                // binding predicate: an optional session on a delivery
                // transport, resolved inside the same transaction.
                auth.orchestrator_session_id
                    .as_deref()
                    .and_then(|session| {
                        tx.query_row(
                            "SELECT transport FROM orchestrator_sessions WHERE id=?",
                            [session],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()
                        .transpose()
                    })
                    .transpose()
                    .ok()
                    .flatten()
                    .filter(|transport| {
                        agent_run_domain::domain::OrchestratorRef::canonical_transport_name(
                            transport,
                        )
                        .is_ok()
                    })
                    .map(|_| auth)
            });
        let Some(auth) = auth else {
            return Err(Error::Validation(
                "worker attempt is not active or bound".into(),
            ));
        };
        let session = auth.orchestrator_session_id.expect("checked above");
        let prior: Option<(String, String, String, String)> = tx
            .query_row(
                "SELECT n.delivery_id,n.kind,n.message,d.state FROM worker_notifications n
             JOIN deliveries d ON d.id=n.delivery_id WHERE n.agent_id=? AND n.request_id=?",
                params![run_id.as_str(), input.request_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        if let Some((notification_id, kind, message, state)) = prior {
            if kind != input.kind.as_str() || message != input.message {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(NotifyReceipt {
                notification_id,
                state,
                duplicate: true,
            });
        }
        let (count, latest): (i64, Option<f64>) = tx.query_row(
            "SELECT COUNT(*),MAX(created_at) FROM worker_notifications WHERE agent_id=?",
            [run_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if count >= MAX_MESSAGES {
            return Err(Error::Validation(
                "worker report limit reached (20 per run)".into(),
            ));
        }
        if latest.is_some_and(|latest| at < latest + MIN_INTERVAL) {
            return Err(Error::Validation(
                "worker reports require 30 seconds between messages".into(),
            ));
        }
        // The event contains metadata only. Worker prose is retained solely in its bounded row.
        tx.execute(
            "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,'worker_notification',?)",
            params![run_id.as_str(), attempt_id, at,
                serde_json::json!({"request_id":input.request_id,"kind":input.kind}).to_string()],
        )?;
        let event = tx.last_insert_rowid();
        let notification_id = format!("ntf_{}", uuid::Uuid::new_v4().simple());
        tx.execute(
            "INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at)
             VALUES(?,?,?,?,'pending',?)",
            params![notification_id, run_id.as_str(), session, event, at],
        )?;
        tx.execute(
            "INSERT INTO worker_notifications(delivery_id,agent_id,attempt_id,request_id,kind,message,created_at)
             VALUES(?,?,?,?,?,?,?)",
            params![notification_id, run_id.as_str(), attempt_id, input.request_id,
                input.kind.as_str(), input.message, at],
        )?;
        // An active pool member also gets exactly one linked team copy of the
        // report and one pending `pool` command per peer tip, all inside this
        // same transaction: an insert failure rolls the notification, the
        // copy and every command back together. The replay path above returns
        // before this point, so a retry never duplicates the copy. The copy
        // lives in the pool's own idempotency scope (`notify:` prefix), so a
        // pool_post reusing the same request key can never collide with it.
        let seat: Option<(String, String, String, u32)> = tx
            .query_row(
                "SELECT p.id,m.name,m.role,p.roster_revision FROM pool_members m \
                 JOIN pools p ON p.id=m.pool_id \
                 WHERE m.agent_id=? AND m.replaced_by IS NULL AND p.state='open'",
                [auth.root_agent_id.as_str()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get::<_, i64>(3)?.max(1) as u32,
                    ))
                },
            )
            .optional()?;
        if let Some((pool_id, name, role, roster_revision)) = seat {
            let scope = format!("notify:{}", run_id.as_str());
            tx.execute(
                "INSERT INTO pool_entries(pool_id,author_kind,author_agent_id,author_name,author_role,\
                 direction,kind,severity,roster_revision,body,delivery_id,sender_run_id,sender_attempt_id,\
                 idem_scope,request_id,created_at) \
                 VALUES(?,'member',?,?,?,'orchestrator_copy','report',?,?,?,?,?,?,?,?,?)",
                params![
                    pool_id,
                    auth.root_agent_id.as_str(),
                    name,
                    role,
                    input.kind.as_str(),
                    roster_revision,
                    input.message,
                    notification_id,
                    run_id.as_str(),
                    attempt_id,
                    scope,
                    input.request_id,
                    at,
                ],
            )?;
            let seq = tx.last_insert_rowid().max(0) as u64;
            crate::pool_log::fanout_entry(&tx, &pool_id, auth.root_agent_id.as_str(), seq)?;
        }
        tx.commit()?;
        Ok(NotifyReceipt {
            notification_id,
            state: "pending".into(),
            duplicate: false,
        })
    }
}
