//! Short-lived, thread-local SQLite connections. Never hold a transaction across await.
use agent_run_config::config::Config;
use agent_run_domain::{
    domain::{self, now, AgentId, Outcome, StartRequest, Status},
    error::invalid,
    Error, Result,
};
use agent_run_platform::{
    fs, process,
    verify::{self, Proof},
};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
pub const VERSION: i64 = 16;
pub const ACTIVE_SQL: &str = "('created','starting','running','cancelling')";
pub struct Store {
    pub conn: Connection,
    pub home: PathBuf,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub id: AgentId,
    pub request: StartRequest,
    pub status: Status,
    pub created_at: f64,
    pub started_at: Option<f64>,
    pub finished_at: Option<f64>,
    pub supervisor_pid: Option<i32>,
    pub supervisor_identity: Option<String>,
    pub supervisor_birth_time: Option<f64>,
    pub process_group_id: Option<i32>,
    pub runtime_session_id: Option<String>,
    pub failure_kind: Option<String>,
    pub failure_text: Option<String>,
    pub exit_code: Option<i32>,
    pub answer_path: Option<PathBuf>,
    pub answer_bytes: Option<u64>,
    pub answer_sha256: Option<String>,
    pub orchestrator_session_id: Option<String>,
    pub parent_agent_id: Option<AgentId>,
    pub root_agent_id: AgentId,
    pub sequence: u32,
    pub resume_of_runtime_session_id: Option<String>,
    pub identity: Option<Value>,
}
impl Record {
    fn read(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        fn parse<T: serde::de::DeserializeOwned>(v: String) -> rusqlite::Result<T> {
            serde_json::from_str(&v).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })
        }
        fn id(v: String) -> rusqlite::Result<AgentId> {
            v.parse().map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })
        }
        let agent_id = id(row.get("id")?)?;
        let root: String = row.get("root_agent_id")?;
        let status: String = row.get("status")?;
        let status = status.parse().map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?;
        Ok(Self {
            id: agent_id.clone(),
            request: parse(row.get("request_json")?)?,
            status,
            created_at: row.get("created_at")?,
            started_at: row.get("started_at")?,
            finished_at: row.get("finished_at")?,
            supervisor_pid: row.get("supervisor_pid")?,
            supervisor_identity: row.get("supervisor_identity")?,
            supervisor_birth_time: row.get("supervisor_birth_time")?,
            process_group_id: row.get("process_group_id")?,
            runtime_session_id: row.get("runtime_session_id")?,
            failure_kind: row.get("failure_kind")?,
            failure_text: row.get("failure_text")?,
            exit_code: row.get("exit_code")?,
            answer_path: row
                .get::<_, Option<String>>("answer_path")?
                .map(PathBuf::from),
            answer_bytes: row.get::<_, Option<i64>>("answer_bytes")?.map(|n| n as u64),
            answer_sha256: row.get("answer_sha256")?,
            orchestrator_session_id: row.get("orchestrator_session_id")?,
            parent_agent_id: row
                .get::<_, Option<String>>("parent_agent_id")?
                .map(id)
                .transpose()?,
            root_agent_id: if root.is_empty() { agent_id } else { id(root)? },
            sequence: row.get("sequence")?,
            resume_of_runtime_session_id: row.get("resume_of_runtime_session_id")?,
            identity: row
                .get::<_, Option<String>>("identity_json")?
                .map(parse)
                .transpose()?,
        })
    }
}
fn tx_event(
    tx: &Transaction<'_>,
    id: &AgentId,
    kind: &str,
    from: Option<Status>,
    to: Option<Status>,
    data: &Value,
) -> Result<i64> {
    tx.execute(
        "INSERT INTO events(agent_id,at,kind,from_status,to_status,data_json) VALUES(?,?,?,?,?,?)",
        params![
            id.as_str(),
            now(),
            kind,
            from.map(Status::as_str),
            to.map(Status::as_str),
            serde_json::to_string(data)?
        ],
    )?;
    Ok(tx.last_insert_rowid())
}
impl Store {
    pub fn initialize(home: &Path) -> Result<Self> {
        fs::private_dir(home)?;
        Self::connect(home, true)
    }
    pub fn open(home: &Path) -> Result<Self> {
        Self::connect(home, false)
    }
    fn connect(home: &Path, create: bool) -> Result<Self> {
        let path = home.join("state.db");
        if let Ok(meta) = std::fs::symlink_metadata(&path) {
            if !meta.is_file() || meta.file_type().is_symlink() {
                return Err(invalid("state.db must be a regular file"));
            }
        }
        if !create && !path.exists() {
            return Err(invalid("state.db is missing; run agent-run init"));
        }
        let conn = Connection::open(&path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "foreign_keys", true)?;
        let version: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if version == 0 && create {
            let tables: i64 = conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table'",
                [],
                |r| r.get(0),
            )?;
            if tables != 0 {
                return Err(invalid(
                    "unversioned nonempty database is not safe to initialize",
                ));
            }
            conn.execute_batch("BEGIN IMMEDIATE")?;
            if let Err(e) = conn.execute_batch(include_str!("../../../sql/schema.sql")) {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(e.into());
            }
            conn.execute_batch("COMMIT")?;
        } else if version != VERSION {
            return Err(invalid(if version > VERSION {
                "database schema is newer than this binary"
            } else {
                "legacy schema requires the upstream migration chain; upgrade a backup to schema 16 before opening it with this port"
            }));
        }
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        Ok(Self {
            conn,
            home: home.to_path_buf(),
        })
    }
    pub fn health(&self) -> Result<Value> {
        let version: i64 = self
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        let integrity: String = self
            .conn
            .query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        let tables: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        Ok(
            json!({"ok":version==VERSION&&integrity=="ok","schema_version":version,"integrity":integrity,"tables":tables}),
        )
    }
    pub fn backup(&self, destination: &Path) -> Result<()> {
        if destination.exists() {
            return Err(invalid("backup destination already exists"));
        }
        self.conn
            .backup(rusqlite::DatabaseName::Main, destination, None)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o600))?;
        Ok(())
    }
    pub fn get(&self, id: &AgentId) -> Result<Record> {
        self.conn
            .query_row(
                "SELECT * FROM agents WHERE id=?",
                [id.as_str()],
                Record::read,
            )
            .optional()?
            .ok_or_else(|| Error::NotFound(id.to_string()))
    }
    /// Read a scoped replay before consulting mutable configuration. The
    /// transaction in admit still resolves concurrent first submissions.
    pub fn replay_request(&self, request: &StartRequest) -> Result<Option<Record>> {
        let Some(id) = request.request_id.as_deref() else {
            return Ok(None);
        };
        let transport = request.orchestrator.as_ref().map(|o| o.transport.as_str());
        let session = request
            .orchestrator
            .as_ref()
            .map(|o| o.external_session_id.as_str());
        Ok(self.conn.query_row("SELECT a.* FROM agents a LEFT JOIN orchestrator_sessions o ON o.id=a.orchestrator_session_id WHERE a.request_id=? AND ((? IS NULL AND a.orchestrator_session_id IS NULL) OR (o.transport=? AND o.external_session_id=?)) ORDER BY a.created_at LIMIT 1",params![id,transport,transport,session],Record::read).optional()?)
    }
    pub fn admit(
        &mut self,
        request: &StartRequest,
        cfg: &Config,
        identity: &Value,
        parent: Option<&Record>,
    ) -> Result<(AgentId, bool)> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = now();
        let session = if let Some(o) = &request.orchestrator {
            let sid = format!("os-{}", uuid::Uuid::new_v4().simple());
            tx.execute("INSERT INTO orchestrator_sessions(id,transport,external_session_id,external_turn_id,created_at,last_seen_at) VALUES(?,?,?,?,?,?) ON CONFLICT(transport,external_session_id) DO UPDATE SET last_seen_at=excluded.last_seen_at,external_turn_id=excluded.external_turn_id",params![sid,o.transport,o.external_session_id,o.external_turn_id,current,current])?;
            Some(tx.query_row(
                "SELECT id FROM orchestrator_sessions WHERE transport=? AND external_session_id=?",
                params![o.transport, o.external_session_id],
                |r| r.get::<_, String>(0),
            )?)
        } else {
            None
        };
        if let Some(rid) = &request.request_id {
            let found=tx.query_row("SELECT * FROM agents WHERE request_id=? AND orchestrator_session_id IS ? ORDER BY created_at LIMIT 1",params![rid,session],Record::read).optional()?;
            if let Some(found) = found {
                let same_fingerprint = identity
                    .get("replay_request_sha256")
                    .and_then(Value::as_str)
                    .zip(
                        found
                            .identity
                            .as_ref()
                            .and_then(|v| v.get("replay_request_sha256"))
                            .and_then(Value::as_str),
                    )
                    .map(|(current, previous)| current == previous);
                if !same_fingerprint.unwrap_or(found.request == *request)
                    || found.parent_agent_id.as_ref() != parent.map(|p| &p.id)
                {
                    return Err(Error::Conflict);
                }
                tx.commit()?;
                return Ok((found.id, false));
            }
        }
        let global: i64 = tx.query_row(
            &format!("SELECT COUNT(*) FROM agents WHERE status IN {ACTIVE_SQL}"),
            [],
            |r| r.get(0),
        )?;
        let runtime_count: i64 = tx.query_row(
            &format!("SELECT COUNT(*) FROM agents WHERE status IN {ACTIVE_SQL} AND runtime=?"),
            [&request.runtime],
            |r| r.get(0),
        )?;
        let runtime = cfg.runtime(&request.runtime)?;
        if global >= cfg.core.max_active_agents as i64
            || runtime
                .max_active_agents
                .is_some_and(|max| runtime_count >= max as i64)
        {
            return Err(Error::Capacity);
        }
        if let Some(parent) = parent {
            let actual = tx.query_row(
                "SELECT * FROM agents WHERE id=?",
                [parent.id.as_str()],
                Record::read,
            )?;
            if !actual.status.terminal() || actual.runtime_session_id.is_none() {
                return Err(invalid(
                    "resume requires a terminal agent with native session identity",
                ));
            }
            let exists: i64 = tx.query_row(
                "SELECT count(*) FROM agents WHERE parent_agent_id=?",
                [parent.id.as_str()],
                |r| r.get(0),
            )?;
            if exists > 0 {
                return Err(Error::Conflict);
            }
        }
        let id = AgentId::new();
        let summary: String = request
            .task
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(160)
            .collect();
        let root = parent
            .map(|p| p.root_agent_id.as_str())
            .unwrap_or(id.as_str());
        tx.execute("INSERT INTO agents(id,request_id,orchestrator_session_id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,parent_agent_id,root_agent_id,sequence,resume_of_runtime_session_id,identity_json) VALUES(?,?,?,?,?,?,?,?,?,?,'starting',?,?,'pending:materialization',?,?,?,?,?)",params![id.as_str(),request.request_id,session,request.runtime,request.model,request.profile,request.task,summary,request.workdir.to_string_lossy(),serde_json::to_string(request)?,current,request.timeout_seconds.unwrap_or(cfg.core.default_timeout_seconds),parent.map(|p|p.id.as_str()),root,parent.map(|p|p.sequence.checked_add(1).ok_or_else(||invalid("lineage sequence overflow"))).transpose()?.unwrap_or(1),parent.and_then(|p|p.runtime_session_id.as_deref()),serde_json::to_string(identity)?])?;
        if let Ok(owner) = process::inspect(std::process::id() as i32) {
            tx.execute("UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=? WHERE id=?",params![serde_json::to_string(&owner)?,owner.birth,id.as_str()])?;
        }
        tx_event(
            &tx,
            &id,
            "status",
            Some(Status::Created),
            Some(Status::Starting),
            &json!({"durable_admission":true}),
        )?;
        tx.commit()?;
        Ok((id, true))
    }
    pub fn event(&self, id: &AgentId, kind: &str, data: &Value) -> Result<()> {
        self.conn.execute(
            "INSERT INTO events(agent_id,at,kind,data_json) VALUES(?,?,?,?)",
            params![id.as_str(), now(), kind, serde_json::to_string(data)?],
        )?;
        Ok(())
    }
    pub fn set_owner(
        &self,
        id: &AgentId,
        pid: i32,
        identity: &str,
        birth: Option<f64>,
    ) -> Result<()> {
        let n=self.conn.execute("UPDATE agents SET supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=? WHERE id=? AND status='starting' AND supervisor_pid IS NULL",params![pid,identity,birth,id.as_str()])?;
        if n != 1 {
            return Err(Error::Conflict);
        }
        self.event(id, "supervisor_ready", &json!({"pid":pid}))
    }
    pub fn update_identity(&self, id: &AgentId, identity: &Value, revision: &str) -> Result<()> {
        let n = self.conn.execute(
            "UPDATE agents SET identity_json=?,config_revision=? WHERE id=? AND status='starting'",
            params![serde_json::to_string(identity)?, revision, id.as_str()],
        )?;
        if n != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    pub fn running(&mut self, id: &AgentId, pgid: i32) -> Result<()> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let s: String =
            tx.query_row("SELECT status FROM agents WHERE id=?", [id.as_str()], |r| {
                r.get(0)
            })?;
        let from: Status = s.parse()?;
        from.transition(Status::Running)?;
        tx.execute(
            "UPDATE agents SET status='running',started_at=?,process_group_id=? WHERE id=?",
            params![now(), pgid, id.as_str()],
        )?;
        tx.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at) VALUES(?,?,1,'running','{}',?)",params![format!("{}:1",id),id.as_str(),now()])?;
        tx_event(
            &tx,
            id,
            "status",
            Some(from),
            Some(Status::Running),
            &json!({}),
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn runtime_session(&self, id: &AgentId, session: &str) -> Result<()> {
        domain::external_id("runtime_session_id", session)?;
        self.conn.execute(
            "UPDATE agents SET runtime_session_id=? WHERE id=?",
            params![session, id.as_str()],
        )?;
        Ok(())
    }
    pub fn message(
        &self,
        id: &AgentId,
        role: &str,
        text: &str,
        name: Option<&str>,
        raw_ref: Option<&str>,
    ) -> Result<()> {
        if text.is_empty() {
            return Ok(());
        }
        if !["user", "assistant", "system", "tool_call", "tool_result"].contains(&role) {
            return Err(invalid("unknown transcript role"));
        }
        self.conn.execute(
            "INSERT INTO messages(agent_id,at,role,name,content,raw_ref) VALUES(?,?,?,?,?,?)",
            params![id.as_str(), now(), role, name, text, raw_ref],
        )?;
        Ok(())
    }
    pub fn finish(
        &mut self,
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
            verify::read(&self.home.join("agents").join(id.as_str()), proof, 0)?;
        }
        let tx = self
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
        if to == Status::Succeeded && proof.is_none() {
            return Err(Error::Integrity("success requires answer proof".into()));
        }
        let time = now();
        tx.execute("UPDATE agents SET status=?,finished_at=?,exit_code=?,failure_kind=?,failure_text=?,runtime_session_id=COALESCE(?,runtime_session_id),answer_path=?,answer_bytes=?,answer_sha256=? WHERE id=?",params![to.as_str(),time,outcome.exit_code,outcome.failure_kind,outcome.failure_text,outcome.runtime_session_id,proof.map(|p|p.path.to_string_lossy().into_owned()),proof.map(|p|p.bytes as i64),proof.map(|p|p.sha256.as_str()),id.as_str()])?;
        tx.execute(
            "UPDATE attempts SET state=?,finished_at=? WHERE agent_id=?",
            params![to.as_str(), time, id.as_str()],
        )?;
        let seq = tx_event(
            &tx,
            id,
            "status",
            Some(from),
            Some(to),
            &json!({"failure_kind":outcome.failure_kind}),
        )?;
        if let Some(session) = row.orchestrator_session_id {
            tx.execute("INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) VALUES(?,?,?,?,'pending',?)",params![format!("ntf_{}",uuid::Uuid::new_v4().simple()),id.as_str(),session,seq,time])?;
        }
        let token = |name: &str| {
            usage
                .and_then(|v| v.get(name))
                .and_then(Value::as_i64)
                .filter(|v| *v >= 0)
        };
        let cost = usage
            .and_then(|v| v.get("cost_usd"))
            .and_then(Value::as_f64)
            .filter(|v| v.is_finite() && *v >= 0.);
        tx.execute("INSERT OR REPLACE INTO run_stats(agent_id,runtime,model,profile,status,failure_kind,started_at,finished_at,duration_seconds,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,reasoning_tokens,total_tokens,num_turns,cost_usd,usage_source,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",params![id.as_str(),row.request.runtime,row.request.model,row.request.profile,to.as_str(),outcome.failure_kind,row.started_at,time,row.started_at.map(|s|(time-s).max(0.)),token("input_tokens"),token("output_tokens"),token("cache_read_tokens"),token("cache_write_tokens"),token("reasoning_tokens"),token("total_tokens"),token("num_turns"),cost,if usage.and_then(|v|v.get("_source")).and_then(Value::as_str)==Some("token_usage_updated"){"token_usage_updated"}else if usage.is_some(){"runtime_result"}else{"none"},time])?;
        tx.commit()?;
        Ok(())
    }
    pub fn enqueue(&mut self, id: &AgentId, kind: &str, payload: &Value) -> Result<Value> {
        if !["cancel", "steer"].contains(&kind) {
            return Err(invalid("unknown command kind"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = tx
            .query_row(
                "SELECT * FROM agents WHERE id=?",
                [id.as_str()],
                Record::read,
            )
            .optional()?
            .ok_or_else(|| Error::NotFound(id.to_string()))?;
        if row.status.terminal() {
            return Err(invalid("agent is already terminal"));
        }
        tx.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,?,?,'pending',?)",params![id.as_str(),kind,serde_json::to_string(payload)?,now()])?;
        let cid = tx.last_insert_rowid();
        tx.commit()?;
        Ok(json!({"command_id":cid,"agent_id":id,"kind":kind,"state":"pending"}))
    }
    pub fn pending_commands(&mut self, id: &AgentId) -> Result<Vec<(i64, String, Value)>> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let commands = {
            let mut stmt=tx.prepare("SELECT id,kind,payload_json FROM commands WHERE agent_id=? AND state='pending' ORDER BY id LIMIT 64")?;
            let rows = stmt
                .query_map([id.as_str()], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            rows
        };
        let mut result = Vec::new();
        for (cid, kind, payload) in commands {
            tx.execute(
                "UPDATE commands SET state='claimed',claimed_at=? WHERE id=? AND state='pending'",
                params![now(), cid],
            )?;
            result.push((cid, kind, serde_json::from_str(&payload)?));
        }
        tx.commit()?;
        Ok(result)
    }
    pub fn command_done(&self, cid: i64, result: &Value) -> Result<()> {
        self.conn.execute("UPDATE commands SET state='completed',completed_at=?,result_json=? WHERE id=? AND state='claimed'",params![now(),serde_json::to_string(result)?,cid])?;
        Ok(())
    }
    pub fn cancel_pending(&self, id: &AgentId) -> Result<bool> {
        Ok(self.conn.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE agent_id=? AND kind='cancel' AND state IN ('pending','claimed'))",[id.as_str()],|r|r.get(0))?)
    }
    pub fn revision(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COALESCE(MAX(seq),0) FROM events", [], |r| r.get(0))?)
    }
    pub fn list(
        &self,
        active: bool,
        offset: usize,
        limit: usize,
        session: Option<&domain::OrchestratorRef>,
    ) -> Result<(Vec<Record>, i64)> {
        if limit == 0 || limit > 1000 {
            return Err(invalid("limit must be 1..1000"));
        }
        let sid=match session{Some(s)=>self.conn.query_row("SELECT id FROM orchestrator_sessions WHERE transport=? AND external_session_id=?",params![s.transport,s.external_session_id],|r|r.get::<_,String>(0)).optional()?,None=>None};
        if session.is_some() && sid.is_none() {
            return Ok((vec![], 0));
        }
        let where_sql = format!(
            "WHERE (?=0 OR status IN {ACTIVE_SQL}) AND (? IS NULL OR orchestrator_session_id=?)"
        );
        let total = self.conn.query_row(
            &format!("SELECT COUNT(*) FROM agents {where_sql}"),
            params![active, sid, sid],
            |r| r.get(0),
        )?;
        let mut stmt = self.conn.prepare(&format!(
            "SELECT * FROM agents {where_sql} ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?"
        ))?;
        let rows = stmt
            .query_map(
                params![active, sid, sid, limit as i64, offset as i64],
                Record::read,
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok((rows, total))
    }
    pub fn transcript(&self, id: &AgentId, cursor: i64, limit: usize) -> Result<Value> {
        self.get(id)?;
        if cursor < 0 || limit == 0 || limit > 1000 {
            return Err(invalid("invalid transcript cursor or limit"));
        }
        let mut stmt=self.conn.prepare("SELECT seq,at,role,name,content,raw_ref FROM messages WHERE agent_id=? AND seq>? ORDER BY seq LIMIT ?")?;
        let mut rows = stmt.query(params![id.as_str(), cursor, limit as i64 + 1])?;
        let mut messages = Vec::new();
        let mut bytes = 0;
        let mut more = false;
        while let Some(row) = rows.next()? {
            let content: String = row.get(4)?;
            if messages.len() >= limit
                || (!messages.is_empty() && bytes + content.len() > 256 * 1024)
            {
                more = true;
                break;
            }
            bytes += content.len();
            messages.push(json!({"seq":row.get::<_,i64>(0)?,"at":row.get::<_,f64>(1)?,"role":row.get::<_,String>(2)?,"name":row.get::<_,Option<String>>(3)?,"content":content,"raw_ref":row.get::<_,Option<String>>(5)?}));
        }
        let next = if more {
            messages.last().and_then(|m| m.get("seq")).cloned()
        } else {
            None
        };
        Ok(
            json!({"agent_id":id,"messages":messages,"cursor":cursor,"limit":limit,"next_cursor":next,"complete":!more}),
        )
    }
    pub fn delivery_status(&self, id: &AgentId) -> Result<Value> {
        let row = self.get(id)?;
        let d=self.conn.query_row("SELECT id,state,attempts,ambiguous_result,last_error FROM deliveries WHERE agent_id=? ORDER BY terminal_event_seq DESC LIMIT 1",[id.as_str()],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,u32>(2)?,r.get::<_,bool>(3)?,r.get::<_,Option<String>>(4)?))).optional()?;
        let (notification_id, state, attempts, ambiguous, last_error, last_attempt) = if let Some(
            (did, state, attempts, ambiguous, last_error),
        ) = d
        {
            let evidence=self.conn.query_row("SELECT evidence_json FROM delivery_attempt_evidence WHERE delivery_id=? ORDER BY attempt DESC LIMIT 1",[&did],|r|r.get::<_,String>(0)).optional()?;
            (
                Some(did),
                state,
                attempts,
                ambiguous,
                last_error,
                evidence
                    .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                    .and_then(|v| safe_evidence(&v)),
            )
        } else {
            (None, "not_created".into(), 0, false, None, None)
        };
        Ok(
            json!({"agent_id":id,"bound":row.orchestrator_session_id.is_some(),"orchestrator_session_id":row.orchestrator_session_id,"notification_id":notification_id,"state":state,"attempts":attempts,"ambiguous":ambiguous,"last_error":last_error,"last_attempt":last_attempt}),
        )
    }
    pub fn last_progress(&self, id: &AgentId) -> Result<Option<f64>> {
        Ok(self.conn.query_row(
            "SELECT MAX(at) FROM messages WHERE agent_id=?",
            [id.as_str()],
            |r| r.get(0),
        )?)
    }
    pub fn last_event(&self, id: &AgentId, kind: &str) -> Result<Option<Value>> {
        let raw=self.conn.query_row("SELECT data_json FROM events WHERE agent_id=? AND kind=? ORDER BY seq DESC LIMIT 1",params![id.as_str(),kind],|r|r.get::<_,String>(0)).optional()?;
        raw.map(|s| serde_json::from_str(&s).map_err(Error::from))
            .transpose()
    }
}

/// Returns a delivery evidence document only when its classifier is safe to expose.
fn safe_evidence(raw: &Value) -> Option<Value> {
    let classifier = raw.get("classifier")?.as_str()?;
    if [
        "relay_accepted",
        "relay_rejected",
        "relay_unavailable",
        "relay_ambiguous",
        "uds_written",
        "session_gone",
        "uds_unavailable",
        "uds_ambiguous",
        "unsupported_transport",
        "delivery_expired",
    ]
    .contains(&classifier)
    {
        Some(raw.clone())
    } else {
        None
    }
}
