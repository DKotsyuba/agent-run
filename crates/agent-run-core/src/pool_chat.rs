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
use rmcp::model::ContentBlock;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::OnceCell,
};

/// Shares the first successful fetch per live attempt.
type Catalog = Arc<OnceCell<Value>>;
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

/// Validates a caller's resume assertion without changing the inherited seat.
pub(crate) fn assert_seat(conn: &Connection, root: &AgentId, expected: &PoolChat) -> Result<()> {
    expected.validate()?;
    let raw: Option<String> = conn
        .query_row(
            "SELECT chat_json FROM lineage_pool_chat WHERE root_agent_id=?",
            [root.as_str()],
            |row| row.get(0),
        )
        .optional()?;
    let stored = raw.as_deref().map(decode_seat).transpose()?;
    if stored.as_ref() != Some(expected) {
        return Err(invalid(
            "pool_chat does not match the inherited lineage seat",
        ));
    }
    Ok(())
}

/// Reads and validates the persisted seat before using it as authority.
fn decode_seat(raw: &str) -> Result<PoolChat> {
    let chat: PoolChat = serde_json::from_str(raw)
        .map_err(|_| Error::Integrity("invalid stored pool_chat seat".into()))?;
    chat.validate()?;
    Ok(chat)
}

/// Identity derived from the broker's durable execution and lineage records.
#[derive(PartialEq, Eq)]
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
        let chat = decode_seat(&raw)?;
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

/// Classifies delivery certainty without exposing paths or peer payloads.
#[derive(Debug)]
enum RpcError {
    Unsent(&'static str),
    Uncertain,
    Rejected(&'static str),
    MemberRefused(&'static str),
    AdmissionPending(u64),
}

impl RpcError {
    /// Reports whether the operation reached Pool or could already have committed.
    fn message(&self) -> &'static str {
        match self {
            Self::Unsent(message) | Self::Rejected(message) | Self::MemberRefused(message) => {
                message
            }
            Self::AdmissionPending(_) => "Agent Pool seat is awaiting admission",
            Self::Uncertain => {
                "Agent Pool did not confirm the outcome. The operation may have committed. Check Pool state before issuing it again."
            }
        }
    }
}

/// One bounded exchange. The write boundary distinguishes a lost ACK from
/// failure before any request bytes could be sent.
async fn rpc(
    path: &Path,
    method: &str,
    parameters: &Value,
) -> std::result::Result<Value, RpcError> {
    const UNAVAILABLE: RpcError =
        RpcError::Unsent("Agent Pool is unavailable. The request was not sent.");
    const UNSUPPORTED: RpcError =
        RpcError::Rejected("Agent Pool does not support chat v1 (upgrade Pool)");
    let id = uuid::Uuid::new_v4().to_string();
    let request = json!({"jsonrpc":"2.0", "id":id, "method":method, "params":parameters});
    let mut encoded = serde_json::to_vec(&request)
        .map_err(|_| RpcError::Unsent("Agent Pool request could not be encoded"))?;
    if encoded.len() >= SOCKET_MAX_FRAME_BYTES {
        return Err(RpcError::Unsent(
            "Agent Pool request exceeds the frame limit",
        ));
    }
    encoded.push(b'\n');
    let mut sending = false;
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut stream = UnixStream::connect(path).await.map_err(|_| UNAVAILABLE)?;
        // SAFETY: geteuid reads the current process credentials with no arguments.
        if stream.peer_cred().map_err(|_| UNAVAILABLE)?.uid() != unsafe { libc::geteuid() } {
            return Err(RpcError::Unsent(
                "Agent Pool socket peer UID does not match Agent Run",
            ));
        }
        sending = true;
        stream
            .write_all(&encoded)
            .await
            .map_err(|_| RpcError::Uncertain)?;
        let bytes = frame::read(&mut BufReader::new(stream), SOCKET_MAX_FRAME_BYTES)
            .await
            .map_err(|_| RpcError::Uncertain)?
            .ok_or(RpcError::Uncertain)?;
        let mut reply: Value = serde_json::from_slice(&bytes).map_err(|_| RpcError::Uncertain)?;
        // Before chat v1, Pool routed unknown methods into managed ingress.
        // Its exact pre-store refusal has no JSON-RPC marker or correlation ID.
        if reply["id"] == ""
            && reply
                == json!({
                    "id":"","error":{"version":1,"code":"peer_not_authorized","recovery":"none"}
                })
        {
            return Err(UNSUPPORTED);
        }
        if reply["jsonrpc"] != "2.0" || reply["id"] != id {
            return Err(RpcError::Uncertain);
        }
        if let Some(error) = reply.get("error") {
            if reply.get("result").is_some() {
                return Err(RpcError::Uncertain);
            }
            let data = &error["data"];
            if error["code"] == -32000
                && data["kind"] == "admission_pending"
                && let Some(delay) = data["retry_after_ms"].as_u64()
            {
                return Err(RpcError::AdmissionPending(delay));
            }
            if !error["code"].is_i64() || !error["message"].is_string() {
                return Err(RpcError::Uncertain);
            }
            if error["code"] == -32601 {
                return Err(UNSUPPORTED);
            }
            if error["code"] == -32000 {
                for kind in ["not_member", "seat_retired", "pool_closed"] {
                    if error["data"]["kind"] == kind {
                        return Err(RpcError::MemberRefused(kind));
                    }
                }
            }
            return Err(RpcError::Rejected(
                "Agent Pool rejected the chat v1 request",
            ));
        }
        let result = reply["result"].take();
        if result["version"] != 1 {
            return Err(RpcError::Rejected(
                "Agent Pool returned an unsupported chat version",
            ));
        }
        Ok(result)
    })
    .await
    .map_err(|_| {
        if sending {
            RpcError::Uncertain
        } else {
            UNAVAILABLE
        }
    })?
}

/// Interprets a member response before deciding whether the original send
/// needs a retry. Validate with the worker SDK and forward Pool’s original content.
async fn member_call(path: &Path, parameters: &Value) -> std::result::Result<Value, RpcError> {
    let mut result = rpc(path, "member_call_v1", parameters).await?;
    let valid_content = result["content"].as_array().is_some_and(|blocks| {
        blocks
            .iter()
            .all(|block| ContentBlock::deserialize(block).is_ok())
    });
    if !valid_content || !result["is_error"].is_boolean() {
        return Err(RpcError::Uncertain);
    }
    Ok(json!({"content":result["content"].take(), "isError":result["is_error"]}))
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
                    return Err(invalid("worker lineage has no Pool chat seat"));
                }
                if let Some(cell) = catalogs.get(&request.attempt_id) {
                    return Ok(cell.clone());
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
                Ok(catalogs.entry(request.attempt_id).or_default().clone())
            })
            .await?;
        let value = cell
            .get_or_try_init(|| async {
                let (config, _) = self
                    .current_provider_config()
                    .map_err(|_| "Agent Pool chat configuration is unavailable")?;
                let endpoint = config.pool_chat.ok_or("Agent Pool chat is disabled")?;
                let result = rpc(&endpoint.socket, "chat_catalog_v1", &json!({"version":1}))
                    .await
                    .map_err(|error| error.message())?;
                catalog(result)
            })
            .await;
        value
            .cloned()
            .map_err(|message| Error::Runtime(message.into()))
    }

    /// Reads an exact running Pool member from a consistent capability snapshot.
    async fn live_pool_member(&self, request: CatalogRequest) -> Result<Member> {
        self.read(move |store| {
            let tx = store.conn().unchecked_transaction()?;
            Member::read(&tx, &request, true)
        })
        .await?
        .ok_or_else(|| invalid("worker lineage has no Pool chat seat"))
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
        let member = self.live_pool_member(request.clone()).await?;
        let (config, _) = self.current_provider_config()?;
        let endpoint = config
            .pool_chat
            .ok_or_else(|| Error::Runtime("Agent Pool chat is disabled".into()))?;
        let request_id = uuid::Uuid::new_v4().to_string();
        let params = member.call(&call.tool, call.input, &request_id);
        let first = member_call(&endpoint.socket, &params).await;
        let retry = matches!(first, Err(RpcError::Uncertain));
        let result = if retry {
            // A lost ACK can follow a committed operation. Retry the same
            // request only while the original execution still has authority.
            if !self
                .live_pool_member(request)
                .await
                .is_ok_and(|current| current == member)
            {
                return Err(Error::Runtime(RpcError::Uncertain.message().into()));
            }
            member_call(&endpoint.socket, &params).await
        } else {
            first
        };
        match result {
            Ok(result) => Ok(result),
            Err(RpcError::MemberRefused(kind)) => {
                let text = if retry {
                    format!("{kind}. {}", RpcError::Uncertain.message())
                } else {
                    kind.to_owned()
                };
                Ok(json!({"content":[{"type":"text","text":text}],
                    "structuredContent":{"kind":kind}, "isError":true}))
            }
            Err(RpcError::AdmissionPending(delay)) if !retry => Ok(json!({
                "content":[{"type":"text","text":format!("Pool seat not ready yet. Retry after ~{delay} ms.")}],
                "structuredContent":{"kind":"admission_pending","retry_after_ms":delay},
                "isError":true
            })),
            Err(_) if retry => Err(Error::Runtime(RpcError::Uncertain.message().into())),
            Err(error) => Err(Error::Runtime(error.message().into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    /// Closing while the first request is in flight preserves its uncertain outcome
    /// and prevents a second send under the retiring worker's capability.
    #[tokio::test]
    async fn pool_chat_does_not_retry_after_attempt_closes() {
        let home = tempfile::tempdir().unwrap();
        let path = home.path().join("pool.sock");
        let listener = UnixListener::bind(&path).unwrap();
        std::fs::write(
            home.path().join("config.toml"),
            format!(
                "schema_version=2\n[pool_chat]\nsocket='{}'\n",
                path.display()
            ),
        )
        .unwrap();
        let mut store = agent_run_store::fixture::initialize(home.path()).unwrap();
        let run_id = AgentId::new();
        let token = "a".repeat(64);
        let request = json!({"runtime":"codex","model":"fixture","profile":"review",
            "task":"task","workdir":home.path(),"explicit_finish":true});
        store.conn.execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision)
             VALUES(?,'codex','fixture','review','task','task',?,?,'running',100,'fixture')",
            params![run_id.as_str(), home.path().to_str().unwrap(), request.to_string()],
        ).unwrap();
        store.conn.execute(
            "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active)
             VALUES('attempt',?,1,'running','{}',100,1)", [run_id.as_str()],
        ).unwrap();
        store
            .issue_worker_capability(&run_id, "attempt", &token, 100.0)
            .unwrap();
        store
            .begin_worker_lifecycle(&run_id, "attempt", 100.0)
            .unwrap();
        let chat = json!({"version":1,"pool_id":"00000000-0000-4000-8000-000000000001",
            "seat_id":"00000000-0000-4000-8000-000000000002",
            "pool_store_id":"store_00000000000000000000000000000003"});
        store
            .conn
            .execute(
                "INSERT INTO lineage_pool_chat VALUES(?,?)",
                params![run_id.as_str(), chat.to_string()],
            )
            .unwrap();
        let service = Service::new(home.path().to_path_buf());
        let relay = service.pool_chat_call(WorkerToolCall {
            run_id,
            attempt_id: "attempt".into(),
            token,
            tool: "pool_post".into(),
            input: json!({}),
        });
        let first_send = async {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            assert!(
                frame::read(&mut reader, SOCKET_MAX_FRAME_BYTES)
                    .await
                    .unwrap()
                    .is_some()
            );
            // While the ACK is outstanding, the Run attempt enters closing.
            store
                .conn
                .execute(
                    "UPDATE worker_lifecycle SET phase='closing',finish_json='{}',finish_sha256=?,accepted_at=101 WHERE attempt_id='attempt'",
                    ["b".repeat(64)],
                )
                .unwrap();
        };
        let (reply, ()) = tokio::time::timeout(Duration::from_secs(8), async {
            tokio::join!(relay, first_send)
        })
        .await
        .unwrap();
        assert!(
            reply
                .unwrap_err()
                .public()
                .message
                .contains("may have committed")
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    }
}
