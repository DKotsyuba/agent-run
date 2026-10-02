//! Typed access to the resident broker for the terminal observer.
//!
//! The TUI owns no store and no processes: every fact on screen comes from one
//! JSON-RPC call against the resident broker socket. The [`Broker`] seam
//! mirrors the CLI broker seam so tests substitute canned responses without a
//! socket.

use agent_run::transport::socket::BrokerClient;
use agent_run_domain::views::{AgentPage, AnswerView, TranscriptPage};
use serde_json::{Value, json};
use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};

/// Upper bound of one transcript page accepted from the broker.
///
/// The broker itself refuses pages above 1000 rows; the observer stays below
/// it so one backfill round is a handful of requests.
pub const TRANSCRIPT_PAGE_LIMIT: usize = 500;

/// Long-poll window, in seconds, of one revision watch round.
pub const REVISION_WAIT_SECONDS: f64 = 25.0;

/// A boxed asynchronous broker call owned by the caller.
pub type BrokerFuture<'a> = Pin<Box<dyn Future<Output = agent_run::Result<Value>> + Send + 'a>>;

/// One broker round trip; the seam used by the [`crate::events`] workers.
pub trait Broker: Send + Sync {
    /// Send one method and JSON object to the resident broker.
    fn call<'a>(&'a self, method: &'a str, params: Value) -> BrokerFuture<'a>;
}

/// Shared dynamic broker seam handed to the event workers.
pub type SharedBroker = Arc<dyn Broker>;

/// Production [`Broker`] backed by the shared framed socket client.
#[derive(Clone)]
pub struct SocketBroker {
    /// Each concurrent request opens its own connection to this local endpoint.
    socket_path: PathBuf,
}

impl SocketBroker {
    /// Creates a lazy broker client; no socket is opened until its first call.
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }
}

impl Broker for SocketBroker {
    /// Keep long-poll listing independent of transcript and answer reads.
    fn call<'a>(&'a self, method: &'a str, params: Value) -> BrokerFuture<'a> {
        let client = BrokerClient::new(self.socket_path.clone());
        Box::pin(async move { client.call(method, Some(params)).await })
    }
}

/// Parses one broker response into a domain view.
fn parse<T: serde::de::DeserializeOwned>(value: Value) -> agent_run::Result<T> {
    Ok(serde_json::from_value(value)?)
}

/// Reads the paged agent snapshot for one scope.
pub async fn list_agents(broker: &dyn Broker, active: bool) -> agent_run::Result<AgentPage> {
    parse(
        broker
            .call(
                "list_agents",
                json!({"active": active, "offset": 0, "limit": 200}),
            )
            .await?,
    )
}

/// Long-polls the agent page until the committed store revision advances.
///
/// The broker answers earlier when the revision moves past `after_revision`,
/// so the returned page is always fresh at delivery time.
pub async fn list_after_revision(
    broker: &dyn Broker,
    after_revision: i64,
    wait_seconds: f64,
) -> agent_run::Result<AgentPage> {
    parse(
        broker
            .call(
                "list_agents",
                json!({
                    "active": false,
                    "offset": 0,
                    "limit": 200,
                    "after_revision": after_revision,
                    "wait_seconds": wait_seconds,
                }),
            )
            .await?,
    )
}

/// Reads one bounded transcript page after the cursor.
pub async fn transcript_page(
    broker: &dyn Broker,
    agent_id: &agent_run_domain::domain::AgentId,
    run_id: Option<&agent_run_domain::domain::AgentId>,
    cursor: i64,
    limit: usize,
) -> agent_run::Result<TranscriptPage> {
    let mut params = json!({"agent_id": agent_id, "cursor": cursor, "limit": limit});
    if let Some(run_id) = run_id {
        params["run_id"] = json!(run_id);
    }
    parse(broker.call("transcript", params).await?)
}

/// Reads the verified answer envelope of one session.
pub async fn answer(
    broker: &dyn Broker,
    agent_id: &agent_run_domain::domain::AgentId,
    run_id: Option<&agent_run_domain::domain::AgentId>,
) -> agent_run::Result<AnswerView> {
    let mut params = json!({"agent_id": agent_id});
    if let Some(run_id) = run_id {
        params["run_id"] = json!(run_id);
    }
    parse(broker.call("answer", params).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_run_domain::domain::{AgentId, Status};

    /// Parses one valid stable id for request arguments.
    fn aid(value: &str) -> AgentId {
        value.parse().expect("valid agent id")
    }
    use serde_json::json;

    /// Broker seam replaying one canned response, failing on other methods.
    struct Canned(Value);

    impl Broker for Canned {
        fn call<'a>(&'a self, _method: &'a str, _params: Value) -> BrokerFuture<'a> {
            Box::pin(async move { Ok(self.0.clone()) })
        }
    }

    #[tokio::test]
    async fn parses_list_agents_page() {
        let broker = Canned(json!({
            "items": [{
                "agent_id": "ag-20260928-101500-aaaaaaaaaa",
                "run_id": "ag-20260928-101500-bbbbbbbbbb",
                "runtime": "codex",
                "model": "gpt-5",
                "profile": "default",
                "task_summary": "ship it",
                "status": "running",
                "created_at": 100.0,
                "started_at": Some(101.0),
                "finished_at": None::<serde_json::Value>,
                "elapsed_seconds": 5.0,
                "last_progress_at": None::<serde_json::Value>,
                "silence_seconds": Some(1.0),
                "warned": false,
                "failure_kind": None::<serde_json::Value>,
                "failure_text": None::<serde_json::Value>,
                "answer_available": false,
                "answer_bytes": None::<serde_json::Value>,
                "answer_sha256": None::<serde_json::Value>,
                "effort": None::<serde_json::Value>,
                "delivery": {
                    "agent_id": "ag-20260928-101500-aaaaaaaaaa",
                    "bound": false,
                    "orchestrator_session_id": None::<serde_json::Value>,
                    "notification_id": None::<serde_json::Value>,
                    "state": "idle",
                    "attempts": 0,
                    "ambiguous": false,
                    "last_error": None::<serde_json::Value>,
                    "last_attempt": None::<serde_json::Value>,
                },
                "parent_agent_id": None::<serde_json::Value>,
                "root_agent_id": "ag-20260928-101500-aaaaaaaaaa",
                "sequence": 1,
                "cleanup": None::<serde_json::Value>,
                "policy": None::<serde_json::Value>,
                "phase": "running",
                "phase_started_at": 101.0,
                "process_state": "alive",
                "observed_at": 105.0,
                "runtime_outcome": None::<serde_json::Value>,
                "acceptance": "pending",
            }],
            "total": 1,
            "offset": 0,
            "limit": 200,
            "next_offset": None::<serde_json::Value>,
            "complete": true,
            "revision": 7,
            "observed_at": 105.0,
        }));
        let page = list_agents(&broker, true).await.expect("page parses");
        assert_eq!(page.revision, 7);
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].status, Status::Running);
        let mut modern = broker.0;
        for field in ["run_id", "root_agent_id", "parent_agent_id"] {
            modern["items"][0].as_object_mut().unwrap().remove(field);
        }
        modern["items"][0]["workdir"] = json!("/tmp/workspace");
        let page = list_agents(&Canned(modern), true)
            .await
            .expect("stable-ID response parses");
        assert!(page.items[0].run_id.is_none());
        assert_eq!(page.items[0].workdir.as_deref(), Some("/tmp/workspace"));
    }

    #[tokio::test]
    async fn parses_transcript_page() {
        let broker = Canned(json!({
            "agent_id": "ag-20260928-101500-aaaaaaaaaa",
            "run_id": "ag-20260928-101500-bbbbbbbbbb",
            "messages": [
                {"seq": 1, "at": 100.0, "role": "user", "name": None::<serde_json::Value>, "content": "hello", "raw_ref": None::<serde_json::Value>},
                {"seq": 2, "at": 101.0, "role": "assistant", "name": None::<serde_json::Value>, "content": "hi", "raw_ref": None::<serde_json::Value>}
            ],
            "cursor": 0,
            "limit": 500,
            "next_cursor": Some(2),
            "complete": false,
        }));
        let page = transcript_page(
            &broker,
            &aid("ag-20260928-101500-aaaaaaaaaa"),
            Some(&aid("ag-20260928-101500-bbbbbbbbbb")),
            0,
            TRANSCRIPT_PAGE_LIMIT,
        )
        .await
        .expect("page parses");
        assert_eq!(page.messages.len(), 2);
        assert_eq!(page.next_cursor, Some(2));
        assert!(!page.complete);
    }

    #[tokio::test]
    async fn answer_params_carry_run_id() {
        struct Capture;
        impl Broker for Capture {
            fn call<'a>(&'a self, _method: &'a str, params: Value) -> BrokerFuture<'a> {
                Box::pin(async move {
                    assert_eq!(params["run_id"], "ag-20260928-101500-bbbbbbbbbb");
                    Ok(json!({
                        "agent_id": "ag-20260928-101500-aaaaaaaaaa",
                        "run_id": "ag-20260928-101500-bbbbbbbbbb",
                        "status": "succeeded",
                        "available": true,
                        "path": "/tmp/answer.md",
                        "size_bytes": 12,
                        "sha256": "abc",
                        "content": "the answer\n",
                        "inline_complete": true,
                        "relative_path": None::<String>,
                        "kind": None::<String>,
                        "media_type": None::<String>,
                        "proof_version": None::<u32>,
                    }))
                })
            }
        }
        let view = answer(
            &Capture,
            &aid("ag-20260928-101500-aaaaaaaaaa"),
            Some(&aid("ag-20260928-101500-bbbbbbbbbb")),
        )
        .await
        .expect("answer parses");
        assert!(view.available);
        assert_eq!(view.content.as_deref(), Some("the answer\n"));
    }
}

/// A pending list watch must not serialize unrelated transcript requests.
#[cfg(test)]
#[tokio::test]
async fn transcript_request_does_not_wait_for_pending_list_watch() {
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
        sync::oneshot,
    };
    let directory = tempfile::Builder::new()
        .prefix("ar-tui-")
        .tempdir_in("/tmp")
        .unwrap();
    let socket = directory.path().join("api.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let (ready, started) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut first = BufReader::new(stream);
        let mut line = String::new();
        first.read_line(&mut line).await.unwrap();
        let request: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(request["method"], "list_agents");
        ready.send(()).unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let mut second = BufReader::new(stream);
        line.clear();
        second.read_line(&mut line).await.unwrap();
        let next: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(next["method"], "transcript");
        for (stream, id) in [
            (second.get_mut(), &next["id"]),
            (first.get_mut(), &request["id"]),
        ] {
            let response = format!(
                "{}\n",
                json!({"jsonrpc":"2.0","id":id,"result":{"ok":true}})
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let broker = Arc::new(SocketBroker::new(socket));
    let watcher = broker.clone();
    let watch = tokio::spawn(async move { watcher.call("list_agents", json!({})).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), started)
        .await
        .unwrap()
        .unwrap();
    let transcript = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        broker.call("transcript", json!({})),
    )
    .await;
    assert!(
        transcript.is_ok(),
        "list watch blocked transcript on the shared connection"
    );
    assert_eq!(transcript.unwrap().unwrap()["ok"], true);
    watch.await.unwrap().unwrap();
    server.await.unwrap();
}
