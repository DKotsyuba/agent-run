//! Stable client-facing projections over the durable journal.

use crate::{ACTIVE_SQL, Record, Store};
use agent_run_domain::{
    Result,
    domain::{AgentId, Status},
    error::invalid,
    views::{
        AgentPage, AgentView, AnswerView, CleanupView, DeliveryView, McpSelectionView, MessageView,
        TranscriptPage, UsageCumulativeView, UsageView,
    },
};
use agent_run_platform::{
    process,
    verify::{self, Proof},
};
use rusqlite::{OptionalExtension, params};
use serde_json::Value;

/// Projects verified frozen role selections without leaking command, env or auth data.
/// Historical identities without a provider role and invalid payloads report no selections.
pub fn selected_mcp(identity: Option<&Value>) -> Vec<McpSelectionView> {
    identity
        .and_then(|v| v.get("authority")?.get("role_payload"))
        .and_then(|v| agent_run_config::role_plan::ResolvedRolePlan::from_payload(v).ok())
        .map(|role| {
            role.mcp
                .into_iter()
                .map(|mcp| McpSelectionView {
                    name: mcp.id,
                    source: mcp.selection.as_str().into(),
                    allowed_tools: mcp.allowed_tools,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Decodes an optional JSON field while treating invalid durable state as a store error.
fn json(value: Option<String>) -> Result<Option<Value>> {
    value
        .map(|raw| serde_json::from_str(&raw).map_err(Into::into))
        .transpose()
}

/// Turns valid cleanup evidence into its transport-safe view; malformed evidence is intentionally absent.
fn cleanup(value: Option<String>) -> Option<CleanupView> {
    let value: Value = serde_json::from_str(&value?).ok()?;
    Some(CleanupView {
        signals: value
            .get("signals")?
            .as_array()?
            .iter()
            .map(Value::as_str)
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .map(str::to_owned)
            .collect(),
        scope: value.get("scope")?.as_str()?.to_owned(),
        group_gone: value.get("group_gone")?.as_bool()?,
        descendants_gone: value.get("descendants_gone").and_then(Value::as_bool),
        confirmed: value.get("confirmed")?.as_bool()?,
        process_group_id: value
            .get("process_group_id")
            .and_then(Value::as_i64)
            .map(|number| number as i32),
    })
}

impl Store {
    /// Builds one current agent view at `observed_at` from committed rows only.
    pub fn agent_view_at(&self, id: &AgentId, observed_at: f64) -> Result<AgentView> {
        if !observed_at.is_finite() || observed_at < 0.0 {
            return Err(invalid("observed_at must be finite and nonnegative"));
        }
        let record = self.get(id)?;
        let progress: Option<f64> = self.conn.query_row(
            "SELECT MAX(at) FROM messages WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )?;
        let warned: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM events WHERE agent_id=? AND kind='deadline_warning')",
            [id.as_str()],
            |row| row.get(0),
        )?;
        let cleanup_json = self.conn.query_row("SELECT data_json FROM events WHERE agent_id=? AND kind='process_cleanup' ORDER BY seq DESC LIMIT 1", [id.as_str()], |row| row.get::<_, String>(0)).optional()?;
        let phase_row = self.conn.query_row("SELECT at,data_json FROM events WHERE agent_id=? AND kind='phase' ORDER BY seq DESC LIMIT 1", [id.as_str()], |row| Ok((row.get::<_, f64>(0)?, row.get::<_, String>(1)?))).optional()?;
        let delivery = self.delivery_view(&record)?;
        let (phase, phase_started_at) = match record.status {
            status if status.terminal() => (
                "terminal".to_owned(),
                record.finished_at.unwrap_or(record.created_at),
            ),
            Status::Cancelling => (
                "stopping".to_owned(),
                record.started_at.unwrap_or(record.created_at),
            ),
            Status::Running => (
                "running".to_owned(),
                record.started_at.unwrap_or(record.created_at),
            ),
            _ => phase_row
                .and_then(|(at, raw)| {
                    serde_json::from_str::<Value>(&raw).ok().and_then(|value| {
                        match value.get("phase").and_then(Value::as_str) {
                            Some("preparing") => Some(("preparing".to_owned(), at)),
                            Some("spawning") => Some(("spawning".to_owned(), at)),
                            _ => None,
                        }
                    })
                })
                .unwrap_or_else(|| ("accepted".to_owned(), record.created_at)),
        };
        let usage = self.usage_view(&record.id)?;
        let usage_cumulative = self.usage_cumulative(&record.root_agent_id)?;
        let process_state = format!(
            "{:?}",
            process::observe(
                record.supervisor_pid,
                record.supervisor_identity.as_deref(),
                record.supervisor_birth_time
            )
        )
        .to_lowercase();
        let end = record.finished_at.unwrap_or(observed_at);
        let silence_seconds = if record.status.terminal() {
            None
        } else {
            Some(
                (observed_at - progress.or(record.started_at).unwrap_or(record.created_at))
                    .max(0.0),
            )
        };
        Ok(AgentView {
            mcp: selected_mcp(record.identity.as_ref()),
            parent_run_id: None,
            run_id: None,
            agent_id: record.id.clone(),
            name: record.display_name.clone(),
            runtime: record.request.runtime,
            model: record.request.model,
            profile: record.request.profile,
            task_summary: self.conn.query_row(
                "SELECT task_summary FROM agents WHERE id=?",
                [id.as_str()],
                |row| row.get(0),
            )?,
            status: record.status,
            created_at: record.created_at,
            started_at: record.started_at,
            finished_at: record.finished_at,
            elapsed_seconds: (end - record.started_at.unwrap_or(record.created_at)).max(0.0),
            last_progress_at: progress,
            silence_seconds,
            warned,
            failure_kind: record.failure_kind,
            failure_text: record.failure_text,
            answer_available: record.answer_path.is_some(),
            answer_bytes: record.answer_bytes,
            answer_sha256: record.answer_sha256,
            effort: record.request.effort,
            delivery,
            parent_agent_id: record.parent_agent_id,
            root_agent_id: Some(record.root_agent_id),
            sequence: record.sequence,
            cleanup: cleanup(cleanup_json),
            policy: record
                .identity
                .and_then(|value| value.get("effective_policy").cloned()),
            phase,
            phase_started_at,
            process_state,
            observed_at,
            runtime_outcome: record
                .status
                .terminal()
                .then(|| record.status.as_str().to_owned()),
            acceptance: "pending".to_owned(),
            workdir: Some(record.request.workdir.display().to_string()),
            usage,
            usage_cumulative: Some(usage_cumulative),
            tool_counts: Some(self.tool_counts(&record.id)?),
        })
    }

    /// Reads one execution's persisted normalized usage row as the public
    /// allowlisted view, or `None` before any statistics row exists.
    ///
    /// Missing measurements stay `null` and `usage_source` is the recorded
    /// protocol name (`none` when no native measurement exists); internal
    /// execution identifiers are never copied into the view.
    pub fn usage_view(&self, id: &AgentId) -> Result<Option<UsageView>> {
        let row = self
            .conn
            .query_row(
                "SELECT input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,reasoning_tokens,total_tokens,num_turns,ttft_ms,api_duration_ms,cost_usd,usage_source,recorded_at FROM run_stats WHERE agent_id=?",
                [id.as_str()],
                |row| {
                    Ok(UsageView {
                        input_tokens: row.get(0)?,
                        output_tokens: row.get(1)?,
                        cache_read_tokens: row.get(2)?,
                        cache_write_tokens: row.get(3)?,
                        reasoning_tokens: row.get(4)?,
                        total_tokens: row.get(5)?,
                        num_turns: row.get(6)?,
                        ttft_ms: row.get(7)?,
                        api_duration_ms: row.get(8)?,
                        cost_usd: row.get(9)?,
                        usage_source: row.get(10)?,
                        recorded_at: row.get(11)?,
                    })
                },
            )
            .optional()?;
        Ok(row)
    }

    /// Aggregates native usage for the logical agent identified by `root`.
    ///
    /// Each metric is summed only when all one-based lineage executions remain
    /// present and each has a recorded measurement. Missing history, missing
    /// statistics rows and null metrics stay unknown. Resumed Codex rows contain
    /// deltas; integer counts are summed without floating-point conversion.
    /// `executions` counts retained rows; an absent lineage has zero rows and
    /// null totals. SQLite errors, including integer overflow, propagate.
    pub fn usage_cumulative(&self, root: &AgentId) -> Result<UsageCumulativeView> {
        let columns = [
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "reasoning_tokens",
            "total_tokens",
            "num_turns",
            "cost_usd",
        ];
        let selects = columns.iter().map(|column| format!(
            "CASE WHEN MIN(l.sequence)=1 AND MAX(l.sequence)=COUNT(*) AND COUNT(s.{column})=COUNT(*) THEN SUM(s.{column}) END"
        )).collect::<Vec<_>>().join(",");
        Ok(self.conn.query_row(
            &format!("SELECT COUNT(*),{selects} FROM agents l LEFT JOIN run_stats s ON s.agent_id=l.id WHERE l.id=?1 OR l.root_agent_id=?1"),
            [root.as_str()],
            |row| Ok(UsageCumulativeView {
                executions: row.get(0)?,
                input_tokens: row.get(1)?,
                output_tokens: row.get(2)?,
                cache_read_tokens: row.get(3)?,
                cache_write_tokens: row.get(4)?,
                reasoning_tokens: row.get(5)?,
                total_tokens: row.get(6)?,
                num_turns: row.get(7)?,
                cost_usd: row.get(8)?,
            }),
        )?)
    }

    /// Returns a stable offset page and the global event revision from one SQLite read transaction.
    pub fn agent_page_at(
        &self,
        active: bool,
        offset: usize,
        limit: usize,
        observed_at: f64,
    ) -> Result<AgentPage> {
        if limit == 0 || limit > 1000 {
            return Err(invalid("limit must be 1..1000"));
        }
        self.conn.execute_batch("BEGIN")?;
        let result = (|| -> Result<AgentPage> {
            let revision = self.revision()?;
            let where_sql = if active {
                format!("WHERE status IN {ACTIVE_SQL}")
            } else {
                String::new()
            };
            let total: i64 = self.conn.query_row(
                &format!("SELECT COUNT(*) FROM agents {where_sql}"),
                [],
                |row| row.get(0),
            )?;
            let mut statement = self.conn.prepare(&format!(
                "SELECT * FROM agents {where_sql} ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?"
            ))?;
            let records = statement
                .query_map(params![limit as i64, offset as i64], Record::read)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let items = records
                .iter()
                .map(|record| self.agent_view_at(&record.id, observed_at))
                .collect::<Result<Vec<_>>>()?;
            let complete = offset + items.len() >= total as usize;
            Ok(AgentPage {
                items,
                total: total as usize,
                offset,
                limit,
                next_offset: (!complete).then_some(offset + records.len()),
                complete,
                revision,
                message_revision: self.message_revision()?,
                observed_at,
            })
        })();
        let _ = self.conn.execute_batch("ROLLBACK");
        result
    }

    /// Paginates a transcript strictly by immutable message sequence without byte-based cursor drift.
    pub fn transcript_page(
        &self,
        id: &AgentId,
        cursor: i64,
        limit: usize,
    ) -> Result<TranscriptPage> {
        if cursor < 0 || limit == 0 || limit > 1000 {
            return Err(invalid("invalid transcript cursor or limit"));
        }
        self.get(id)?;
        let mut statement = self.conn.prepare("SELECT seq,at,role,name,content,raw_ref,error,error_source,content_complete FROM messages WHERE agent_id=? AND seq>? ORDER BY seq LIMIT ?")?;
        let rows = statement
            .query_map(params![id.as_str(), cursor, limit as i64 + 1], |row| {
                Ok(MessageView {
                    seq: row.get(0)?,
                    at: row.get(1)?,
                    role: row.get(2)?,
                    name: row.get(3)?,
                    content: row.get(4)?,
                    raw_ref: row.get(5)?,
                    error: row.get(6)?,
                    error_source: row.get(7)?,
                    content_complete: row.get(8)?,
                    ..MessageView::default()
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let complete = rows.len() <= limit;
        let messages = rows.into_iter().take(limit).collect::<Vec<_>>();
        let next_cursor = (!complete)
            .then(|| messages.last().map(|message| message.seq))
            .flatten();
        Ok(TranscriptPage {
            run_id: None,
            agent_id: id.clone(),
            messages,
            cursor,
            limit,
            next_cursor,
            complete,
            view: None,
            direction: None,
            before_cursor: None,
            previous_cursor: None,
            resume_cursor: None,
        })
    }

    /// Verifies stored answer metadata and returns bounded inline content when the sealed payload permits it.
    pub fn answer_view(&self, id: &AgentId) -> Result<AnswerView> {
        let record = self.get(id)?;
        let Some(path) = record.answer_path.clone() else {
            return Ok(AnswerView {
                run_id: None,
                agent_id: id.clone(),
                status: record.status,
                available: false,
                path: None,
                size_bytes: None,
                sha256: None,
                content: None,
                inline_complete: true,
                relative_path: None,
                kind: None,
                media_type: None,
                proof_version: None,
            });
        };
        let bytes = record
            .answer_bytes
            .ok_or_else(|| invalid("stored answer proof is incomplete"))?;
        let sha256 = record
            .answer_sha256
            .clone()
            .ok_or_else(|| invalid("stored answer proof is incomplete"))?;
        let root = self.home.join("agents").join(id.as_str());
        let proof = Proof {
            path: path.clone(),
            bytes,
            sha256: sha256.clone(),
            proof_version: 2,
        };
        let (proof_version, content) = verify::read(&root, &proof, verify::INLINE_ANSWER)?;
        let relative = path
            .strip_prefix(&root)
            .map_err(|_| invalid("stored answer path is outside the agent directory"))?
            .to_string_lossy()
            .into_owned();
        Ok(AnswerView {
            run_id: None,
            agent_id: id.clone(),
            status: record.status,
            available: true,
            path: Some(path),
            size_bytes: Some(bytes),
            sha256: Some(sha256),
            inline_complete: content.is_some(),
            content,
            relative_path: Some(relative),
            kind: Some("agent_answer".to_owned()),
            media_type: Some(verify::MEDIA_TYPE.to_owned()),
            proof_version: Some(proof_version),
        })
    }

    /// Returns the persisted normalized run-statistics row without inventing missing measurements.
    pub fn run_statistics(&self, id: &AgentId) -> Result<Option<Value>> {
        self.conn.query_row("SELECT runtime,model,profile,status,failure_kind,started_at,finished_at,duration_seconds,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,reasoning_tokens,total_tokens,num_turns,ttft_ms,api_duration_ms,cost_usd,usage_source,recorded_at FROM run_stats WHERE agent_id=?", [id.as_str()], |row| Ok(serde_json::json!({
            "agent_id": id.as_str(), "runtime": row.get::<_, String>(0)?, "model": row.get::<_, String>(1)?, "profile": row.get::<_, String>(2)?, "status": row.get::<_, String>(3)?, "failure_kind": row.get::<_, Option<String>>(4)?, "started_at": row.get::<_, Option<f64>>(5)?, "finished_at": row.get::<_, Option<f64>>(6)?, "duration_seconds": row.get::<_, Option<f64>>(7)?, "input_tokens": row.get::<_, Option<i64>>(8)?, "output_tokens": row.get::<_, Option<i64>>(9)?, "cache_read_tokens": row.get::<_, Option<i64>>(10)?, "cache_write_tokens": row.get::<_, Option<i64>>(11)?, "reasoning_tokens": row.get::<_, Option<i64>>(12)?, "total_tokens": row.get::<_, Option<i64>>(13)?, "num_turns": row.get::<_, Option<i64>>(14)?, "ttft_ms": row.get::<_, Option<f64>>(15)?, "api_duration_ms": row.get::<_, Option<f64>>(16)?, "cost_usd": row.get::<_, Option<f64>>(17)?, "usage_source": row.get::<_, String>(18)?, "recorded_at": row.get::<_, f64>(19)?
        }))).optional().map_err(Into::into)
    }

    /// Reads the latest individual completion delivery and safe evidence; worker,
    /// common-pool and enrollment attention notices never imply task completion.
    fn delivery_view(&self, record: &Record) -> Result<DeliveryView> {
        let delivery = self.conn.query_row("SELECT id,state,attempts,ambiguous_result,last_error FROM deliveries WHERE agent_id=? AND id NOT IN (SELECT delivery_id FROM worker_notifications) AND id NOT IN (SELECT completion_delivery_id FROM pools WHERE completion_delivery_id IS NOT NULL) AND terminal_event_seq NOT IN (SELECT seq FROM events WHERE kind='pool_join_needs_action') ORDER BY terminal_event_seq DESC LIMIT 1", [record.id.as_str()], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, u32>(2)?, row.get::<_, bool>(3)?, row.get::<_, Option<String>>(4)?))).optional()?;
        let Some((notification_id, state, attempts, ambiguous, last_error)) = delivery else {
            return Ok(DeliveryView {
                run_id: None,
                agent_id: record.id.clone(),
                bound: record.orchestrator_session_id.is_some(),
                orchestrator_session_id: record.orchestrator_session_id.clone(),
                notification_id: None,
                state: "not_created".to_owned(),
                attempts: 0,
                ambiguous: false,
                last_error: None,
                last_attempt: None,
            });
        };
        let evidence = self.conn.query_row("SELECT evidence_json FROM delivery_attempt_evidence WHERE delivery_id=? ORDER BY attempt DESC LIMIT 1", [notification_id.as_str()], |row| row.get::<_, String>(0)).optional()?;
        Ok(DeliveryView {
            run_id: None,
            agent_id: record.id.clone(),
            bound: record.orchestrator_session_id.is_some(),
            orchestrator_session_id: record.orchestrator_session_id.clone(),
            notification_id: Some(notification_id),
            state,
            attempts,
            ambiguous,
            last_error,
            last_attempt: json(evidence)?,
        })
    }
}
