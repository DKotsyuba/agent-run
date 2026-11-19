//! Authenticated worker-to-Pool relay. No worker credential leaves Run.
use crate::{Error, Result, error::invalid, service::Service};
use agent_run_domain::{
    StoreId,
    domain::AgentId,
    observation::SOCKET_MAX_FRAME_BYTES,
    pool_chat::{CatalogRequest, PoolChat, TOOLS},
    worker::WorkerToolCall,
};
use agent_run_platform::frame;
use agent_run_store::{store_identity, worker::authenticate_attempt};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{io::BufReader, net::UnixStream, sync::OnceCell};

/// Shares a single fetch (including failures) per live attempt.
type Catalog = Arc<OnceCell<std::result::Result<Value, &'static str>>>;
pub(crate) type CatalogCache = Arc<Mutex<HashMap<String, Catalog>>>;

/// Checks the seat of an exact execution, including descendants of the root.
pub(crate) fn has_seat(conn: &Connection, execution: &AgentId) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM agents a JOIN lineage_pool_chat c
         ON c.root_agent_id=COALESCE(NULLIF(a.root_agent_id,''),a.id) WHERE a.id=?)",
        [execution.as_str()],
        |row| row.get(0),
    )?)
}

/// Identity derived from the broker's durable execution and lineage records.
struct Member {
    run_store_id: StoreId,
    agent_id: AgentId,
    sequence: u32,
    chat: PoolChat,
}

impl Member {
    /// Fetches the seat for an authenticated attempt. Catalog startup also
    /// accepts STARTING workers, which initialize MCP before Run marks running.
    fn read(conn: &Connection, request: &CatalogRequest, running: bool) -> Result<Option<Self>> {
        let auth = authenticate_attempt(
            conn,
            &request.run_id,
            &request.attempt_id,
            &request.token,
            crate::domain::now(),
        )?
        .filter(|auth| !running || auth.running)
        .ok_or_else(|| invalid("invalid or inactive worker capability"))?;
        let row: Option<(u32, String)> = conn
            .query_row(
                "SELECT a.sequence,c.chat_json FROM agents a
                 JOIN lineage_pool_chat c ON c.root_agent_id=?1 WHERE a.id=?2",
                params![auth.root_agent_id.as_str(), request.run_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((sequence, raw)) = row else {
            return Ok(None);
        };
        let chat: PoolChat = serde_json::from_str(&raw)
            .map_err(|_| Error::Integrity("invalid stored pool_chat seat".into()))?;
        chat.validate()?;
        Ok(Some(Self {
            run_store_id: store_identity::read(conn)?,
            agent_id: auth.root_agent_id,
            sequence,
            chat,
        }))
    }

    /// Builds the request independently of any similarly named model arguments.
    fn call(&self, tool: &str, arguments: Value, request_id: &str) -> Value {
        json!({
            "version": 1, "run_store_id": self.run_store_id, "agent_id": self.agent_id,
            "sequence": self.sequence, "pool_id": self.chat.pool_id, "seat_id": self.chat.seat_id,
            "tool": tool, "arguments": arguments, "request_id": request_id,
        })
    }
}

/// Retryable connectivity failures use a closed, redacted diagnostic.
const UNAVAILABLE: &str = "Agent Pool is unavailable";

/// A bounded exchange with a same-UID Unix peer. Errors omit socket paths and payloads.
async fn rpc(
    path: &Path,
    method: &str,
    parameters: &Value,
) -> std::result::Result<Value, &'static str> {
    const INVALID: &str = "Agent Pool returned an invalid chat v1 response";
    let id = uuid::Uuid::new_v4().to_string();
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut stream = UnixStream::connect(path).await.map_err(|_| UNAVAILABLE)?;
        // SAFETY: geteuid reads the current process credentials with no arguments.
        if stream.peer_cred().map_err(|_| UNAVAILABLE)?.uid() != unsafe { libc::geteuid() } {
            return Err("Agent Pool socket peer UID does not match Agent Run");
        }
        let request = json!({"jsonrpc":"2.0", "id":id, "method":method, "params":parameters});
        frame::write(&mut stream, &request, SOCKET_MAX_FRAME_BYTES)
            .await
            .map_err(|_| UNAVAILABLE)?;
        let bytes = frame::read(&mut BufReader::new(stream), SOCKET_MAX_FRAME_BYTES)
            .await
            .map_err(|_| UNAVAILABLE)?
            .ok_or(UNAVAILABLE)?;
        let mut reply: Value = serde_json::from_slice(&bytes).map_err(|_| INVALID)?;
        if reply["jsonrpc"] != "2.0" || reply["id"] != id {
            return Err(INVALID);
        }
        if let Some(error) = reply.get("error") {
            if reply.get("result").is_some() {
                return Err(INVALID);
            }
            if error["code"] == -32601 {
                return Err("Agent Pool does not support chat v1 (method not found)");
            }
            for kind in ["not_member", "seat_retired", "pool_closed"] {
                if error["message"] == kind
                    || error["data"]["kind"] == kind
                    || error["data"]["code"] == kind
                {
                    return Err(kind);
                }
            }
            return Err("Agent Pool rejected the chat v1 request");
        }
        let result = reply["result"].take();
        if result["version"] != 1 {
            return Err("Agent Pool returned an unsupported chat version");
        }
        Ok(result)
    })
    .await
    .map_err(|_| UNAVAILABLE)?
}

/// Verifies the exact v1 inventory and forwards Pool's schema objects unchanged.
fn catalog(mut result: Value) -> std::result::Result<Value, &'static str> {
    let tools = result["tools"]
        .as_array()
        .ok_or("invalid Agent Pool tool catalog")?;
    let names = tools
        .iter()
        .filter(|tool| tool["description"].is_string() && tool["inputSchema"].is_object())
        .filter_map(|tool| tool["name"].as_str())
        .collect::<HashSet<_>>();
    if tools.len() != TOOLS.len() || !TOOLS.iter().all(|name| names.contains(name)) {
        return Err("invalid Agent Pool tool catalog");
    }
    Ok(result["tools"].take())
}

impl Service {
    /// Loads schemas once at MCP startup. Unpooled workers never contact Pool.
    pub(crate) async fn pool_chat_catalog(&self, request: CatalogRequest) -> Result<Value> {
        let catalogs = self.pool_catalogs.clone();
        let cell = self
            .read(move |store| {
                let mut catalogs = catalogs
                    .lock()
                    .map_err(|_| Error::Runtime("Pool catalog cache is unavailable".into()))?;
                let tx = store.conn().unchecked_transaction()?;
                if Member::read(&tx, &request, false)?.is_none() {
                    return Ok(None);
                }
                if let Some(cell) = catalogs.get(&request.attempt_id) {
                    return Ok(Some(cell.clone()));
                }
                // Never evict an active attempt's original catalog, including when
                // its MCP reconnects. Retired attempts need no explicit teardown.
                let mut statement = tx.prepare(
                    "SELECT id FROM attempts WHERE ownership_active=1 AND finished_at IS NULL",
                )?;
                let live = statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<HashSet<_>>>()?;
                catalogs.retain(|attempt, _| live.contains(attempt));
                Ok(Some(
                    catalogs.entry(request.attempt_id).or_default().clone(),
                ))
            })
            .await?;
        let Some(cell) = cell else {
            return Ok(json!([]));
        };
        let value = cell
            .get_or_init(|| async {
                let (config, _) = self
                    .current_provider_config()
                    .map_err(|_| "Agent Pool chat configuration is unavailable")?;
                let endpoint = config.pool_chat.ok_or("Agent Pool chat is disabled")?;
                catalog(rpc(&endpoint.socket, "chat_catalog_v1", &json!({"version":1})).await?)
            })
            .await;
        value
            .clone()
            .map_err(|message| Error::Runtime(message.into()))
    }

    /// Authenticates the exact running attempt and relays opaque model arguments.
    pub(crate) async fn pool_chat_call(&self, call: WorkerToolCall) -> Result<Value> {
        if !call.input.is_object() {
            return Err(invalid("pool chat arguments must be an object"));
        }
        let request = CatalogRequest {
            run_id: call.run_id,
            attempt_id: call.attempt_id,
            token: call.token,
        };
        let member = self
            .read(move |store| {
                let tx = store.conn().unchecked_transaction()?;
                Member::read(&tx, &request, true)
            })
            .await?
            .ok_or_else(|| invalid("worker lineage has no Pool chat seat"))?;
        let (config, _) = self.current_provider_config()?;
        let endpoint = config
            .pool_chat
            .ok_or_else(|| Error::Runtime("Agent Pool chat is disabled".into()))?;
        let request_id = uuid::Uuid::new_v4().to_string();
        let params = member.call(&call.tool, call.input, &request_id);
        let mut result = rpc(&endpoint.socket, "member_call_v1", &params).await;
        if result == Err(UNAVAILABLE) {
            // A lost response can follow a committed operation. Preserve its id.
            result = rpc(&endpoint.socket, "member_call_v1", &params).await;
        }
        let mut result = result.map_err(|message| Error::Runtime(message.into()))?;
        if !result["content"].is_array() || !result["is_error"].is_boolean() {
            return Err(Error::Runtime("invalid Agent Pool tool result".into()));
        }
        Ok(json!({"content":result["content"].take(), "isError":result["is_error"]}))
    }
}
