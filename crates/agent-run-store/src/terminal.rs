//! Atomic terminal lifecycle commits.
//!
//! A terminal state, its event, answer proof metadata, and exactly one durable
//! completion-delivery row are written under one immediate SQLite transaction.

use crate::{delivery, tx_event, Record, Store};
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
    let token = |name: &str| {
        usage
            .and_then(|value| value.get(name))
            .and_then(Value::as_i64)
            .filter(|value| *value >= 0)
    };
    let cost = usage
        .and_then(|value| value.get("cost_usd"))
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value >= 0.0);
    let usage_source = if usage
        .and_then(|value| value.get("_source"))
        .and_then(Value::as_str)
        == Some("token_usage_updated")
    {
        "token_usage_updated"
    } else if usage.is_some() {
        "runtime_result"
    } else {
        "none"
    };
    tx.execute(
        "INSERT OR REPLACE INTO run_stats(agent_id,runtime,model,profile,status,failure_kind,started_at,finished_at,duration_seconds,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,reasoning_tokens,total_tokens,num_turns,cost_usd,usage_source,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        params![id.as_str(), row.request.runtime, row.request.model, row.request.profile, to.as_str(), outcome.failure_kind, row.started_at, time, row.started_at.map(|started| (time - started).max(0.)), token("input_tokens"), token("output_tokens"), token("cache_read_tokens"), token("cache_write_tokens"), token("reasoning_tokens"), token("total_tokens"), token("num_turns"), cost, usage_source, time],
    )?;
    tx.commit()?;
    Ok(())
}
