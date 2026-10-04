//! Typed access to the resident broker for the terminal observer.
//!
//! The TUI owns no store and no processes: every fact on screen comes from one
//! JSON-RPC call against the resident broker socket. The [`Broker`] seam
//! mirrors the CLI broker seam so tests substitute canned responses without a
//! socket.

use agent_run::transport::socket::BrokerClient;
use agent_run_domain::views::{AgentPage, AnswerView, TranscriptPage};
use serde_json::{json, Value};
use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};

/// Upper bound of one transcript page accepted from the broker.
///
/// The broker itself refuses pages above 1000 rows; the observer uses its
/// maximum so one backfill round is a handful of requests.
pub const TRANSCRIPT_PAGE_LIMIT: usize = 1000;

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

/// Production broker with independent sessions, transcript, answer and pool sockets.
/// Dropping an incomplete call retires its socket before it can serve a new call.
pub struct SocketBroker {
    /// Socket endpoint for lazy replacements after cancellation.
    socket: PathBuf,
    /// Persistent connection per worker lane (list, transcript, one-shot, pools).
    clients: [std::sync::Mutex<Arc<BrokerClient>>; 4],
}

impl SocketBroker {
    /// Creates four lazy clients; no socket opens before its first call.
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        let socket = socket_path.into();
        Self {
            clients: std::array::from_fn(|_| {
                std::sync::Mutex::new(Arc::new(BrokerClient::new(socket.clone())))
            }),
            socket,
        }
    }
}

/// Retires a connection if its future is dropped before a response arrives.
struct InFlight<'a> {
    /// Lane to replace on cancellation.
    lane: &'a std::sync::Mutex<Arc<BrokerClient>>,
    /// Endpoint of the replacement client.
    socket: &'a PathBuf,
    /// Whether the response has been consumed.
    complete: bool,
}

impl Drop for InFlight<'_> {
    /// A cancelled request may leave unread frames, so never reuse its socket.
    fn drop(&mut self) {
        if !self.complete {
            *self.lane.lock().expect("broker lane") =
                Arc::new(BrokerClient::new(self.socket.clone()));
        }
    }
}

impl Broker for SocketBroker {
    /// Routes each method to its worker's connection and retires cancelled calls.
    fn call<'a>(&'a self, method: &'a str, params: Value) -> BrokerFuture<'a> {
        let index = match method {
            "list_agents" => 0,
            "transcript" => 1,
            "pool" | "list_pools" => 3,
            _ => 2,
        };
        Box::pin(async move {
            let lane = &self.clients[index];
            let client = lane.lock().expect("broker lane").clone();
            let mut flight = InFlight {
                lane,
                socket: &self.socket,
                complete: false,
            };
            let result = client.call(method, Some(params)).await;
            flight.complete = true;
            result
        })
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

/// Reads the unfiltered session total without loading rows.
///
/// Omitting `active` widens the broker's filter to every session, so the
/// page's exact `total` counts live and finished rows together while
/// `limit: 1` keeps the round trip cheap.
pub async fn total_agents(broker: &dyn Broker) -> agent_run::Result<AgentPage> {
    parse(
        broker
            .call("list_agents", json!({"offset": 0, "limit": 1}))
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

/// Reads a validated blocks request with the domain's shared bounds. Reverse
/// options use cursor zero; identity and run pinning match raw requests.
pub async fn transcript_blocks(
    broker: &dyn Broker,
    agent_id: &agent_run_domain::domain::AgentId,
    run_id: Option<&agent_run_domain::domain::AgentId>,
    query: agent_run_domain::transcript::TranscriptQuery,
) -> agent_run::Result<TranscriptPage> {
    query.validate()?;
    let mut params = serde_json::to_value(query)?;
    params["agent_id"] = json!(agent_id);
    if let Some(run_id) = run_id {
        params["run_id"] = json!(run_id);
    }
    parse(broker.call("transcript", params).await?)
}

/// Recognizes a broker rejecting new blocks parameters. Transport, storage and
/// unrelated validation failures must retry without silently changing views.
pub fn blocks_unsupported(error: &agent_run::Error) -> bool {
    let reason = error.to_string().to_lowercase();
    ["view", "blocks", "tail_blocks", "before_cursor"]
        .iter()
        .any(|key| reason.contains(key))
        && [
            "unknown",
            "unsupported",
            "unexpected",
            "unrecognized",
            "invalid",
            "not supported",
        ]
        .iter()
        .any(|word| reason.contains(word))
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
    }

    /// The finished-count call asks for one unfiltered row: no `active`
    /// filter, so the broker's exact total covers every session.
    #[tokio::test]
    async fn total_agents_fetches_one_unfiltered_row() {
        struct Capture;
        impl Broker for Capture {
            fn call<'a>(&'a self, _method: &'a str, params: Value) -> BrokerFuture<'a> {
                Box::pin(async move {
                    assert!(params.get("active").is_none(), "no live filter: {params}");
                    assert_eq!(params["offset"], 0);
                    assert_eq!(params["limit"], 1, "one row keeps the round cheap");
                    Ok(json!({
                        "items": [],
                        "total": 103, "offset": 0, "limit": 1,
                        "next_offset": Some(1), "complete": false,
                        "revision": 9, "observed_at": 1.0,
                    }))
                })
            }
        }
        let page = total_agents(&Capture).await.expect("count page parses");
        assert_eq!(page.total, 103);
        assert_eq!(page.revision, 9);
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
    /// A blocked list poll cannot delay transcript, answer or pool reads on live JSON-RPC sockets.
    /// Blocked pools leave transcript/answer lanes free; cancelled pool and transcript reads
    /// retire their unread connections before the next call.
    #[tokio::test]
    async fn socket_lanes_stay_independent_and_cancelled_frames_are_retired() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let socket = std::env::temp_dir().join(format!(
            "artui-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (seen_tx, mut seen_rx) = tokio::sync::mpsc::channel::<String>(8);
        let server = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let tx = seen_tx.clone();
                handlers.spawn(async move {
                    let (input, mut output) = stream.into_split();
                    let mut input = BufReader::new(input);
                    let mut line = String::new();
                    while input.read_line(&mut line).await.unwrap() > 0 {
                        let request: Value = serde_json::from_str(&line).unwrap();
                        let method = request["method"].as_str().unwrap().to_string();
                        tx.send(method.clone()).await.unwrap();
                        if method == "list_agents" || request["params"]["blocked"] == true {
                            std::future::pending::<()>().await;
                        }
                        let response =
                            json!({"jsonrpc":"2.0","id":request["id"],"result":{"ok":true}})
                                .to_string()
                                + "\n";
                        output.write_all(response.as_bytes()).await.unwrap();
                        line.clear();
                    }
                });
            }
        });
        let broker = Arc::new(SocketBroker::new(socket.clone()));
        let listing = {
            let broker = broker.clone();
            tokio::spawn(async move { broker.call("list_agents", json!({})).await })
        };
        assert_eq!(seen_rx.recv().await.unwrap(), "list_agents");
        for method in ["transcript", "answer", "pool", "list_pools"] {
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(500),
                broker.call(method, json!({})),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(result, json!({"ok":true}));
            assert_eq!(seen_rx.recv().await.unwrap(), method);
        }
        let pool = {
            let broker = broker.clone();
            tokio::spawn(async move { broker.call("pool", json!({"blocked":true})).await })
        };
        assert_eq!(seen_rx.recv().await.unwrap(), "pool");
        for method in ["transcript", "answer"] {
            assert!(tokio::time::timeout(
                std::time::Duration::from_millis(500),
                broker.call(method, json!({}))
            )
            .await
            .unwrap()
            .is_ok());
            assert_eq!(seen_rx.recv().await.unwrap(), method);
        }
        pool.abort();
        let _ = pool.await;
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(500),
            broker.call("list_pools", json!({}))
        )
        .await
        .unwrap()
        .is_ok());
        assert_eq!(seen_rx.recv().await.unwrap(), "list_pools");
        let stale = {
            let broker = broker.clone();
            tokio::spawn(async move { broker.call("transcript", json!({"blocked":true})).await })
        };
        assert_eq!(seen_rx.recv().await.unwrap(), "transcript");
        stale.abort();
        let _ = stale.await;
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(500),
            broker.call("transcript", json!({}))
        )
        .await
        .unwrap()
        .is_ok());
        listing.abort();
        let _ = listing.await;
        server.abort();
        let _ = server.await;
        std::fs::remove_file(socket).unwrap();
    }

    /// A live Unix JSON-RPC lane transmits validated tail, older and forward
    /// block queries and parses the shared response without custom wire types.
    #[tokio::test]
    async fn block_queries_smoke_over_live_socket() {
        use crate::tests_support::block_page;
        use agent_run_domain::transcript::{TranscriptQuery, TranscriptView};
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let socket = std::env::temp_dir().join(format!(
            "artui-block-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (input, mut output) = stream.into_split();
            let mut input = BufReader::new(input);
            for round in 0..3 {
                let mut line = String::new();
                input.read_line(&mut line).await.unwrap();
                let request: Value = serde_json::from_str(&line).unwrap();
                let params = &request["params"];
                assert_eq!(request["method"], "transcript");
                assert_eq!(params["view"], "blocks");
                assert_eq!(params["run_id"], "ag-20260928-101500-bbbbbbbbbb");
                match round {
                    0 => {
                        assert_eq!(params["tail_blocks"], 40);
                        assert_eq!(params["cursor"], 0);
                    }
                    1 => {
                        assert_eq!(params["before_cursor"], 100);
                        assert_eq!(params["cursor"], 0);
                    }
                    _ => {
                        assert_eq!(params["cursor"], 150);
                        assert!(params["before_cursor"].is_null());
                    }
                }
                let page = block_page(vec![], (round == 1).then_some(100), None, 150, round != 2);
                let response =
                    json!({"jsonrpc":"2.0","id":request["id"],"result":page}).to_string() + "\n";
                output.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let broker = SocketBroker::new(socket.clone());
        let agent = aid("ag-20260928-101500-aaaaaaaaaa");
        let run = aid("ag-20260928-101500-bbbbbbbbbb");
        for round in 0..3 {
            let query = TranscriptQuery {
                cursor: if round == 2 { 150 } else { 0 },
                limit: 200,
                view: TranscriptView::Blocks,
                tail_blocks: (round == 0).then_some(40),
                before_cursor: (round == 1).then_some(100),
            };
            let page = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                transcript_blocks(&broker, &agent, Some(&run), query),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(page.resume_cursor, Some(150));
        }
        server.await.unwrap();
        std::fs::remove_file(socket).unwrap();
    }
}
