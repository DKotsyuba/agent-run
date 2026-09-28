//! Capability-bound worker reports sharing the existing delivery outbox.

use crate::Store;
use agent_run_domain::{
    domain::AgentId,
    worker::{NotifyReceipt, NotifyRequest},
    Error, Result,
};
use agent_run_platform::fs::sha256;
use rusqlite::{params, OptionalExtension, TransactionBehavior};

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
        let route: Option<(String, String)> = tx
            .query_row(
                "SELECT c.token_sha256,a.orchestrator_session_id FROM worker_capabilities c
             JOIN attempts t ON t.id=c.attempt_id JOIN agents a ON a.id=t.agent_id
             JOIN orchestrator_sessions s ON s.id=a.orchestrator_session_id
             WHERE t.id=? AND t.agent_id=? AND t.ownership_active=1 AND t.finished_at IS NULL
               AND t.state='running' AND a.status='running' AND a.finished_at IS NULL
               AND a.created_at+a.timeout_seconds>?
               AND s.transport IN ('codex_queue','claude_uds')",
                params![attempt_id, run_id.as_str(), at],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((digest, session)) = route else {
            return Err(Error::Validation(
                "worker attempt is not active or bound".into(),
            ));
        };
        if digest != sha256(token.as_bytes()) {
            return Err(Error::Validation("invalid worker capability".into()));
        }
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
        tx.commit()?;
        Ok(NotifyReceipt {
            notification_id,
            state: "pending".into(),
            duplicate: false,
        })
    }
}
