//! Atomic terminal lifecycle commits.
//!
//! A terminal state, its event, answer proof metadata, and exactly one durable
//! completion-delivery row are written under one immediate SQLite transaction.

use crate::{Record, Store, delivery, run_stats, tx_event};
use agent_run_domain::{
    Result,
    domain::{AgentId, Outcome, Status, now},
    error::invalid,
};
use agent_run_platform::verify::{self, Proof};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};

/// Commits one terminal result and any individual completion notice it needs.
///
/// The answer proof is verified before opening the transaction. Once started,
/// the state update, attempt close, event append, answer metadata, eligible
/// outbox row, and aggregate run statistics either commit together or SQLite
/// rolls all of them back. Confirmed legacy cleanup releases ownership in this
/// transaction; unconfirmed attempts remain owned for recovery. Successful current pool members
/// omit the individual notice because pool settlement owns the common success
/// notice. A pending cancel observed while success or an explicit-mode outcome
/// is being committed wins in this same transaction and receives its terminal
/// command result. Repeating a completion after a prior terminal commit is a
/// no-op.
/// Explicit success also requires an observed done finish; any supplied answer
/// proof must match that current attempt's immutable callback summary.
pub fn finish(
    store: &mut Store,
    id: &AgentId,
    outcome: &Outcome,
    proof: Option<&Proof>,
    usage: Option<&Value>,
) -> Result<()> {
    if !outcome.status.terminal() {
        return Err(invalid("outcome must be terminal"));
    }
    if outcome.status == Status::Succeeded && proof.is_none() {
        return Err(invalid("success requires a sealed answer proof"));
    }
    if let Some(proof) = proof {
        verify::read(&store.home.join("agents").join(id.as_str()), proof, 0)?;
    }
    let tx = store
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row = tx.query_row(
        "SELECT * FROM agents WHERE id=?",
        [id.as_str()],
        Record::read,
    )?;
    if row.status.terminal() {
        return Ok(());
    }
    if row.request.explicit_finish {
        let intent: Option<(String,bool)> = tx.query_row(
            "SELECT l.finish_json,l.receipt_observed FROM worker_lifecycle l JOIN attempts t ON t.id=l.attempt_id \
             WHERE t.agent_id=? AND t.ownership_active=1 AND t.finished_at IS NULL AND l.finish_json IS NOT NULL",
            [id.as_str()],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        let intent = intent
            .map(|(payload, observed)| {
                serde_json::from_str::<agent_run_domain::worker::FinishRequest>(&payload)
                    .map(|intent| (intent, observed))
            })
            .transpose()?;
        if outcome.status == Status::Succeeded
            && !intent.as_ref().is_some_and(|(intent, observed)| {
                *observed && intent.status == agent_run_domain::worker::FinishStatus::Done
            })
        {
            return Err(invalid(
                "explicit success requires an observed private done finish receipt",
            ));
        }
        if let Some(proof) = proof {
            let (intent, _) =
                intent.ok_or_else(|| invalid("explicit answer requires a finish intent"))?;
            if proof.sha256 != agent_run_platform::fs::sha256(intent.summary.as_bytes())
                || proof.bytes != intent.summary.len() as u64
            {
                return Err(invalid("answer does not match immutable finish summary"));
            }
        }
    }
    let provider_attempt: Option<String> = if row
        .identity
        .as_ref()
        .is_some_and(|value| value["provider_identity_version"] == 2)
    {
        let proof: (String, Option<String>) = tx.query_row(
            "SELECT id,cleanup_proof_json FROM attempts WHERE agent_id=? AND ownership_active=1",
            [id.as_str()],
            |item| Ok((item.get(0)?, item.get(1)?)),
        )?;
        if proof.1.is_none() {
            return Err(invalid(
                "provider attempt cannot release without cleanup proof",
            ));
        }
        Some(proof.0)
    } else {
        None
    };
    let pending_cancel = if outcome.status == Status::Succeeded || row.request.explicit_finish {
        tx.query_row(
            "SELECT id FROM commands WHERE agent_id=? AND kind='cancel' AND state='pending' ORDER BY id LIMIT 1",
            [id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
    } else {
        None
    };
    let terminal_cancel = row.status == Status::Running && pending_cancel.is_some();
    let requested_status = if terminal_cancel {
        Status::Cancelled
    } else {
        outcome.status
    };
    let mut from = row.status;
    if from == Status::Running && requested_status == Status::Cancelled {
        tx_event(
            &tx,
            id,
            "status",
            Some(from),
            Some(Status::Cancelling),
            &json!({}),
        )?;
        from = Status::Cancelling;
    }
    let to = if from == Status::Cancelling && requested_status != Status::Cancelled {
        Status::Lost
    } else {
        requested_status
    };
    from.transition(to)?;
    let time = now();
    tx.execute(
        "UPDATE agents SET status=?,finished_at=?,exit_code=?,failure_kind=?,failure_text=?,runtime_session_id=COALESCE(?,runtime_session_id),answer_path=?,answer_bytes=?,answer_sha256=? WHERE id=?",
        params![to.as_str(), time, outcome.exit_code, outcome.failure_kind, outcome.failure_text, outcome.runtime_session_id, proof.map(|proof| proof.path.to_string_lossy().into_owned()), proof.map(|proof| proof.bytes as i64), proof.map(|proof| proof.sha256.as_str()), id.as_str()],
    )?;
    if let Some(command_id) = pending_cancel {
        tx.execute(
            "UPDATE commands SET state='completed',claimed_at=?,completed_at=?,result_json=? WHERE id=? AND state='pending'",
            params![
                time,
                time,
                serde_json::to_string(&json!({"accepted":true,"reason":"terminal_cancel"}))?,
                command_id
            ],
        )?;
    }
    if let Some(attempt) = &provider_attempt {
        tx.execute(
            "UPDATE attempts SET state=?,finished_at=? WHERE id=? AND agent_id=? AND ownership_active=1",
            params![to.as_str(), time, attempt, id.as_str()],
        )?;
    } else {
        tx.execute(
            "UPDATE attempts SET state=?,finished_at=? WHERE agent_id=?",
            params![to.as_str(), time, id.as_str()],
        )?;
    }
    let event = tx_event(
        &tx,
        id,
        "status",
        Some(from),
        Some(to),
        &json!({"failure_kind":outcome.failure_kind}),
    )?;
    delivery::insert_terminal_notice(&tx, id, row.orchestrator_session_id.as_deref(), event, time)?;
    if let Some(usage) = usage {
        let kind = if usage.get("_source").and_then(Value::as_str) == Some("token_usage_updated") {
            "thread/tokenUsage/updated"
        } else {
            "runtime_result"
        };
        tx_event(&tx, id, kind, None, None, usage)?;
    }
    // Statistics are an observability snapshot, not part of the terminal
    // lifecycle proof. A malformed or unavailable stats table must not turn a
    // committed engine outcome back into an active row.
    let _stats_error = run_stats::record_in_transaction(&tx, id, time).is_err();
    if let Some(attempt) = provider_attempt {
        tx.execute(
            "UPDATE attempts SET ownership_active=0 WHERE id=? AND agent_id=?",
            params![attempt, id.as_str()],
        )?;
    } else {
        // Legacy attempts release only confirmed cleanup, atomically with the terminal row.
        // Unconfirmed ownership stays active for reconciliation, even after a failed run.
        tx.execute(
            "UPDATE attempts SET ownership_active=0 WHERE agent_id=? AND ownership_active=1 \
             AND phase='cleanup_complete' AND json_extract(cleanup_proof_json,'$.confirmed')=1",
            [id.as_str()],
        )?;
    }
    tx.commit()?;
    // Incident diagnostics cannot undo the durable outcome. Retention retries
    // this projection before it is allowed to destroy the source history.
    let _ = crate::incidents::capture(&store.conn, id.as_str(), time);
    Ok(())
}
