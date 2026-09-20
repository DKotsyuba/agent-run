//! Python-compatible normalization and backfill of durable run statistics.
//!
//! Runtime event payloads are intentionally retained in their native shapes;
//! this module is the single boundary that turns those payloads into the one
//! queryable `run_stats` row for an agent. Missing or malformed measurements
//! remain SQL `NULL` and are never represented as zero.

use crate::{Record, Store};
use agent_run_domain::{domain::AgentId, Result};
use rusqlite::{params, Transaction, TransactionBehavior};
use serde_json::Value;

/// One normalized, nullable run-usage snapshot ready for SQLite insertion.
#[derive(Clone, Copy)]
struct Stats {
    /// Source protocol that supplied this snapshot, or `none` when unavailable.
    usage_source: &'static str,
    /// Prompt/input token count, when reported.
    input_tokens: Option<f64>,
    /// Generated/output token count, when reported.
    output_tokens: Option<f64>,
    /// Read-from-cache token count, when reported.
    cache_read_tokens: Option<f64>,
    /// Written-to-cache token count, when reported.
    cache_write_tokens: Option<f64>,
    /// Reasoning/thinking token count, when reported.
    reasoning_tokens: Option<f64>,
    /// Runtime-reported total token count, when reported.
    total_tokens: Option<f64>,
    /// Runtime-reported turn count, when reported.
    num_turns: Option<f64>,
    /// First-token latency in milliseconds, when reported.
    ttft_ms: Option<f64>,
    /// API duration in milliseconds, when reported.
    api_duration_ms: Option<f64>,
    /// Runtime-reported USD cost, when reported.
    cost_usd: Option<f64>,
}

/// Reads a non-boolean JSON number without coercing strings or nulls.
fn number(value: &Value) -> Option<f64> {
    value.as_f64().filter(|number| number.is_finite())
}

/// Reads one nested non-boolean JSON number, preserving missing-path absence.
fn nested_number(value: &Value, keys: &[&str]) -> Option<f64> {
    let mut value = value;
    for key in keys {
        value = value.get(*key)?;
    }
    number(value)
}

/// Returns a snapshot whose every measurement is deliberately unavailable.
fn empty() -> Stats {
    Stats {
        usage_source: "none",
        input_tokens: None,
        output_tokens: None,
        cache_read_tokens: None,
        cache_write_tokens: None,
        reasoning_tokens: None,
        total_tokens: None,
        num_turns: None,
        ttft_ms: None,
        api_duration_ms: None,
        cost_usd: None,
    }
}

/// Normalizes the Claude/GLM `runtime_result` payload shape.
fn runtime_result(payload: &Value) -> Stats {
    Stats {
        usage_source: "runtime_result",
        input_tokens: nested_number(payload, &["usage", "input_tokens"]),
        output_tokens: nested_number(payload, &["usage", "output_tokens"]),
        cache_read_tokens: nested_number(payload, &["usage", "cache_read_input_tokens"]),
        cache_write_tokens: nested_number(payload, &["usage", "cache_creation_input_tokens"]),
        reasoning_tokens: nested_number(
            payload,
            &["usage", "output_tokens_details", "thinking_tokens"],
        ),
        total_tokens: None,
        num_turns: payload.get("num_turns").and_then(number),
        ttft_ms: payload.get("ttft_ms").and_then(number),
        api_duration_ms: payload.get("duration_api_ms").and_then(number),
        cost_usd: payload.get("total_cost_usd").and_then(number),
    }
}

/// Normalizes Codex's cumulative `thread/tokenUsage/updated` payload shape.
fn token_usage(payload: &Value) -> Stats {
    let total = payload.pointer("/tokenUsage/total").unwrap_or(&Value::Null);
    Stats {
        usage_source: "token_usage_updated",
        input_tokens: total.get("inputTokens").and_then(number),
        output_tokens: total.get("outputTokens").and_then(number),
        cache_read_tokens: total.get("cachedInputTokens").and_then(number),
        cache_write_tokens: total.get("cacheWriteInputTokens").and_then(number),
        reasoning_tokens: total.get("reasoningOutputTokens").and_then(number),
        total_tokens: total.get("totalTokens").and_then(number),
        num_turns: None,
        ttft_ms: None,
        api_duration_ms: None,
        cost_usd: None,
    }
}

/// Subtracts a resumed-thread baseline from its latest cumulative counters.
fn resumed_token_usage(current: Option<&Value>, baseline: Option<&Value>) -> Stats {
    let (Some(current), Some(baseline)) = (current, baseline) else {
        return empty();
    };
    let current = token_usage(current);
    let baseline = token_usage(baseline);
    let subtract = |current: Option<f64>, baseline: Option<f64>| match (current, baseline) {
        (Some(current), Some(baseline)) if current >= baseline => Some(current - baseline),
        _ => None,
    };
    Stats {
        usage_source: "token_usage_updated",
        input_tokens: subtract(current.input_tokens, baseline.input_tokens),
        output_tokens: subtract(current.output_tokens, baseline.output_tokens),
        cache_read_tokens: subtract(current.cache_read_tokens, baseline.cache_read_tokens),
        cache_write_tokens: subtract(current.cache_write_tokens, baseline.cache_write_tokens),
        reasoning_tokens: subtract(current.reasoning_tokens, baseline.reasoning_tokens),
        total_tokens: subtract(current.total_tokens, baseline.total_tokens),
        num_turns: None,
        ttft_ms: None,
        api_duration_ms: None,
        cost_usd: None,
    }
}

/// Parses relevant durable events in sequence order and retains each last payload.
fn usage_events(
    tx: &Transaction<'_>,
    id: &AgentId,
) -> Result<(Option<Value>, Option<Value>, Option<Value>)> {
    let mut statement = tx.prepare(
        "SELECT kind,data_json FROM events WHERE agent_id=? AND kind IN ('runtime_result','thread/tokenUsage/updated','resume_usage_baseline') ORDER BY seq",
    )?;
    let rows = statement.query_map([id.as_str()], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut runtime = None;
    let mut current = None;
    let mut baseline = None;
    for row in rows {
        let (kind, raw) = row?;
        let Ok(payload) = serde_json::from_str::<Value>(&raw) else {
            continue;
        };
        if !payload.is_object() {
            continue;
        }
        match kind.as_str() {
            "runtime_result" => runtime = Some(payload),
            "thread/tokenUsage/updated" => current = Some(payload),
            "resume_usage_baseline" => baseline = Some(payload),
            _ => unreachable!("usage query filters kinds"),
        }
    }
    Ok((runtime, current, baseline))
}

/// Obtains lifecycle timestamps from their event transitions, matching Python exactly.
fn transition_times(tx: &Transaction<'_>, id: &AgentId) -> Result<(Option<f64>, Option<f64>)> {
    let mut statement = tx.prepare(
        "SELECT at,to_status FROM events WHERE agent_id=? AND to_status IS NOT NULL ORDER BY seq",
    )?;
    let rows = statement.query_map([id.as_str()], |row| {
        Ok((row.get::<_, f64>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut started = None;
    let mut finished = None;
    for row in rows {
        let (at, status) = row?;
        if status == "running" && started.is_none() {
            started = Some(at);
        }
        if ["succeeded", "failed", "timed_out", "cancelled", "lost"].contains(&status.as_str()) {
            finished = Some(at);
        }
    }
    Ok((started, finished))
}

/// Recomputes one agent's normalized row within a caller-owned transaction.
pub(crate) fn record_in_transaction(
    tx: &Transaction<'_>,
    id: &AgentId,
    recorded_at: f64,
) -> Result<()> {
    let record = tx.query_row(
        "SELECT * FROM agents WHERE id=?",
        [id.as_str()],
        Record::read,
    )?;
    let (runtime, current, baseline) = usage_events(tx, id)?;
    let stats = if let Some(runtime) = runtime.as_ref() {
        runtime_result(runtime)
    } else if record.resume_of_runtime_session_id.is_some() {
        resumed_token_usage(current.as_ref(), baseline.as_ref())
    } else {
        current.as_ref().map_or_else(empty, token_usage)
    };
    let (started_at, finished_at) = transition_times(tx, id)?;
    let duration_seconds = started_at
        .zip(finished_at)
        .map(|(start, finish)| (finish - start).max(0.0));
    tx.execute(
        "INSERT OR REPLACE INTO run_stats(agent_id,runtime,model,profile,status,failure_kind,started_at,finished_at,duration_seconds,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,reasoning_tokens,total_tokens,num_turns,ttft_ms,api_duration_ms,cost_usd,usage_source,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        params![id.as_str(), record.request.runtime, record.request.model, record.request.profile, record.status.as_str(), record.failure_kind, started_at, finished_at, duration_seconds, stats.input_tokens, stats.output_tokens, stats.cache_read_tokens, stats.cache_write_tokens, stats.reasoning_tokens, stats.total_tokens, stats.num_turns, stats.ttft_ms, stats.api_duration_ms, stats.cost_usd, stats.usage_source, recorded_at],
    )?;
    Ok(())
}

/// Idempotently records one current statistics row from its durable source events.
pub fn record(store: &mut Store, id: &AgentId) -> Result<()> {
    let tx = store
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    record_in_transaction(&tx, id, agent_run_domain::domain::now())?;
    tx.commit()?;
    Ok(())
}

/// Backfills agents missing statistics, counting independently failed rows as skipped.
pub fn backfill(store: &mut Store) -> Result<(usize, usize)> {
    let mut statement = store.conn.prepare(
        "SELECT agents.id FROM agents LEFT JOIN run_stats ON run_stats.agent_id=agents.id WHERE run_stats.agent_id IS NULL ORDER BY agents.created_at,agents.id",
    )?;
    let ids = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    let mut backfilled = 0;
    let mut skipped = 0;
    for raw in ids {
        let Ok(id) = raw.parse::<AgentId>() else {
            skipped += 1;
            continue;
        };
        match record(store, &id) {
            Ok(()) => backfilled += 1,
            Err(_) => skipped += 1,
        }
    }
    Ok((backfilled, skipped))
}
