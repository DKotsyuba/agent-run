//! Atomic terminal lifecycle commits.
//!
//! A terminal state, its event, answer proof metadata, and exactly one durable
//! completion-delivery row are written under one immediate SQLite transaction.

use crate::{delivery, run_stats, tx_event, Record, Store};
use agent_run_domain::{
    domain::{now, AgentId, Outcome, Status},
    error::invalid,
    Result,
};
use agent_run_platform::verify::{self, Proof};
use rusqlite::{params, TransactionBehavior};
use serde_json::{json, Value};

/// Commits one terminal result and the completion notice that announces it.
///
/// The answer proof is verified before opening the transaction. Once started,
/// the state update, attempt close, event append, answer metadata, outbox row,
/// and aggregate run statistics either commit together or SQLite rolls all of
/// them back. Repeating a completion after a prior terminal commit is a no-op.
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
    let mut from = row.status;
    if from == Status::Running && outcome.status == Status::Cancelled {
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
    let to = if from == Status::Cancelling && outcome.status != Status::Cancelled {
        Status::Lost
    } else {
        outcome.status
    };
    from.transition(to)?;
    let time = now();
    tx.execute(
        "UPDATE agents SET status=?,finished_at=?,exit_code=?,failure_kind=?,failure_text=?,runtime_session_id=COALESCE(?,runtime_session_id),answer_path=?,answer_bytes=?,answer_sha256=? WHERE id=?",
        params![to.as_str(), time, outcome.exit_code, outcome.failure_kind, outcome.failure_text, outcome.runtime_session_id, proof.map(|proof| proof.path.to_string_lossy().into_owned()), proof.map(|proof| proof.bytes as i64), proof.map(|proof| proof.sha256.as_str()), id.as_str()],
    )?;
    tx.execute(
        "UPDATE attempts SET state=?,finished_at=? WHERE agent_id=?",
        params![to.as_str(), time, id.as_str()],
    )?;
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
    run_stats::record_in_transaction(&tx, id, time)?;
    tx.commit()?;
    Ok(())
}
