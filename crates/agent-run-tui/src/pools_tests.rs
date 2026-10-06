//! Pool-tab regression fixtures, broker replay and deterministic terminal frames.
use super::*;
use crate::{
    events::{self, Action as Global, Pipeline, UiEvent},
    tests_support::{FakeBroker, agent_view, force_truecolor},
};
use agent_run_domain::pool::{AuthorKind, Direction, EntryKind};
use ratatui::{
    Terminal,
    backend::TestBackend,
    crossterm::event::{Event, KeyEvent, KeyModifiers},
};
use serde_json::Value;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

/// Broker stub exposing exact method calls and a controllable pool reconnect generation.
struct DiscoveryBroker {
    /// Recorded pool-lane method names.
    calls: Mutex<Vec<String>>,
    /// Simulated pool socket generation.
    generation: AtomicU64,
    /// Whether the legacy discovery method is unavailable.
    legacy: bool,
    /// Whether list_pools should return a genuine operational error.
    real_error: bool,
}

impl crate::net::Broker for DiscoveryBroker {
    /// Records calls and serves compact discovery/status fixtures.
    fn call<'a>(&'a self, method: &'a str, params: Value) -> crate::net::BrokerFuture<'a> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(method.to_owned());
            match method {
                "list_pools" if self.real_error => {
                    Err(agent_run::Error::Runtime("daemon overloaded".into()))
                }
                "list_pools" if self.legacy && self.generation.load(Ordering::Relaxed) == 0 => Err(
                    agent_run::Error::Runtime("JSON-RPC method not found".into()),
                ),
                "list_pools" => Ok(serde_json::to_value(empty_list(
                    params["offset"].as_u64().unwrap_or(0) as usize,
                ))
                .unwrap()),
                "pool" => {
                    let id = params["pool_id"].as_str().unwrap();
                    if id == "pool-20261004-100002-2222222222" {
                        return Err(agent_run::Error::NotFound(id.into()));
                    }
                    let mode = if id == "pool-20261004-100001-1111111111" {
                        "completed"
                    } else {
                        "active"
                    };
                    let mut value = page_value(mode, vec![], Some(i64::MAX as u64), true);
                    value["pool_id"] = json!(id);
                    Ok(value)
                }
                _ => Err(agent_run::Error::Runtime(format!(
                    "unexpected method {method}"
                ))),
            }
        })
    }

    /// Returns the simulated pool socket generation.
    fn pool_connection_generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }
}

/// Session summaries contribute only strictly shaped ids; explicit ids are retained.
#[test]
fn candidates_parse_strict_pool_ids_and_dedupe() {
    let mut agent = agent_view(
        "ag-20261004-100000-aaaaaaaaaa",
        "ag-20261004-100000-aaaaaaaaaa",
        "running",
    );
    agent.task_summary =
        format!("member of cooperative pool {ID} and pool-20261004-100000-7ac03b9e1Z");
    assert_eq!(candidates(&[agent], None), vec![ID.parse().unwrap()]);
}

/// Missing discovery falls back once, confirms candidates, partitions state, and retries after reconnect.
#[tokio::test]
async fn discovery_fallback_confirms_pools_and_retries_after_reconnect() {
    let broker = Arc::new(DiscoveryBroker {
        calls: Mutex::new(vec![]),
        generation: AtomicU64::new(0),
        legacy: true,
        real_error: false,
    });
    let mut session = agent_view(
        "ag-20261004-100000-aaaaaaaaaa",
        "ag-20261004-100000-aaaaaaaaaa",
        "running",
    );
    session.task_summary =
        format!("pool {ID} pool-20261004-100001-1111111111 pool-20261004-100002-2222222222");
    let (request_tx, request_rx) = tokio::sync::watch::channel(Request {
        visible: true,
        candidates: candidates(&[session], None),
        ..Request::default()
    });
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let worker = tokio::spawn(super::worker(broker.clone(), request_rx, tx));
    for expected_fallback in [true, true] {
        let event = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let BrokerEvent::Pools {
            page: Ok((open, completed)),
            fallback,
            ..
        } = event
        else {
            panic!("expected discovery event")
        };
        assert_eq!(fallback, expected_fallback);
        if expected_fallback {
            assert_eq!(
                open.items
                    .iter()
                    .map(|p| p.pool_id.to_string())
                    .collect::<Vec<_>>(),
                vec![ID]
            );
            assert_eq!(completed.items.len(), 1);
            assert_eq!(completed.items[0].state, PoolState::Completed);
            assert!(
                !open
                    .items
                    .iter()
                    .any(|p| p.pool_id.as_str().contains("222222"))
            );
        }
    }
    assert_eq!(
        broker
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.as_str() == "list_pools")
            .count(),
        1
    );
    broker.generation.store(1, Ordering::Relaxed);
    let event = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let BrokerEvent::Pools { fallback, .. } = event else {
        panic!("expected discovery event")
    };
    assert!(!fallback);
    assert_eq!(
        broker
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.as_str() == "list_pools")
            .count(),
        3
    );
    drop(request_tx);
    worker.abort();
}

/// A genuine list failure remains a broker error and does not enable compatibility mode.
#[tokio::test]
async fn discovery_real_errors_remain_visible_errors() {
    let broker = Arc::new(DiscoveryBroker {
        calls: Mutex::new(vec![]),
        generation: AtomicU64::new(0),
        legacy: false,
        real_error: true,
    });
    let (_request_tx, request_rx) = tokio::sync::watch::channel(Request {
        visible: true,
        ..Request::default()
    });
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let worker = tokio::spawn(super::worker(broker, request_rx, tx));
    let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let BrokerEvent::Pools {
        page: Err(error),
        fallback,
        ..
    } = event
    else {
        panic!("expected real discovery error")
    };
    assert!(!fallback);
    assert!(error.contains("daemon overloaded"));
    let mut app = App::new();
    app.pools.visible = true;
    app.pools.error = Some(error);
    assert!(frame(&app, 120, 30).contains("broker error, retrying · daemon overloaded"));
    worker.abort();
}

/// Stable illustrative pool identity from the design frames.
const ID: &str = "pool-20261004-100000-7ac03b9e12";
/// Four current stable member identities.
const MEMBERS: [&str; 4] = [
    "ag-20261004-100000-aaaaaaaaaa",
    "ag-20261004-100000-bbbbbbbbbb",
    "ag-20261004-100000-cccccccccc",
    "ag-20261004-100000-eeeeeeeeee",
];
/// Retired stable member identity.
const RETIRED: &str = "ag-20261004-100000-dddddddddd";

/// Fixture status for active, waiting-last-vote or completed with exact broker fields.
fn status(mode: &str) -> Value {
    let names = ["Ada", "Ben", "Cy", "Eli"];
    let roles = ["implement", "verify", "review", "verify"];
    let members = MEMBERS.iter().enumerate().map(|(i, id)| {
        let ready = mode == "completed" || mode == "waiting" && i < 3 || mode == "active" && i == 0;
        let succeeded = mode == "completed" || mode == "waiting" && i < 3;
        json!({"agent_id":id,"slot":i+1,"name":names[i],"role":roles[i],
            "tip_status":if succeeded {"succeeded"} else {"running"},
            "cleanup_complete":succeeded,"counts":ready,
            "vote":if ready {json!({"decision":"ready"})} else if i == 2 && mode == "active" {json!({"decision":"block"})} else {Value::Null},
            "why":if ready {"valid"} else if i == 2 && mode == "active" {"blocked"} else {"missing"}})
    }).collect::<Vec<_>>();
    json!({"state":if mode == "completed" {"completed"} else {"open"},
        "roster_revision":2,"goal":"Ship reliable pool observation",
        "criteria":[{"id":"tests","text":"pass"},{"id":"layout","text":"fits"},{"id":"evidence","text":"honest"}],
        "current_proposal":if mode == "empty" {Value::Null} else {json!({"seq":20,"snapshot":"Sessions + pool log; votes and cleanup stay distinct.","roster_revision":2})},
        "members":members,"replaced_members":[{"agent_id":RETIRED,"slot":4,"name":"Dana","role":"verify","replaced_by":MEMBERS[3]}],
        "agreed":mode == "completed","note":"formal checks only"})
}
/// Valid member message; tests adapt author/kind fields for each public entry category.
fn entry(seq: u64, body: &str) -> PoolEntryView {
    PoolEntryView {
        seq,
        roster_revision: 2,
        author_kind: AuthorKind::Member,
        author_agent_id: Some(MEMBERS[0].parse().unwrap()),
        author_name: Some("Ada".into()),
        author_role: Some("implement".into()),
        direction: Direction::Team,
        kind: EntryKind::Message,
        severity: None,
        proposal_seq: None,
        decision: None,
        body: body.into(),
    }
}
/// Illustrative full conversation with historical roster, operator, proposal, reports and votes.
fn entries(mode: &str) -> Vec<PoolEntryView> {
    let mut roster = entry(10, "slot 4 Dana replaced by Eli.");
    roster.author_kind = AuthorKind::Broker;
    roster.author_agent_id = None;
    roster.author_name = None;
    roster.author_role = None;
    roster.kind = EntryKind::Roster;
    let mut operator = roster.clone();
    operator.seq = 18;
    operator.author_kind = AuthorKind::Operator;
    operator.kind = EntryKind::Message;
    operator.body = "Keep the observer read-only.".into();
    let mut proposal = entry(20, "Ship the pool observer.");
    proposal.kind = EntryKind::Proposal;
    let mut vote = entry(21, "Tests and layout checked.");
    vote.kind = EntryKind::Vote;
    vote.proposal_seq = Some(20);
    vote.decision = Some(VoteDecision::Ready);
    let mut report = entry(22, "Frame golden tests still need narrow coverage.");
    report.author_agent_id = Some(MEMBERS[1].parse().unwrap());
    report.author_name = Some("Ben".into());
    report.author_role = Some("verify".into());
    report.kind = EntryKind::Report;
    report.direction = Direction::OrchestratorCopy;
    report.severity = Some(agent_run_domain::worker::WorkerMessageKind::Risk);
    let mut out = vec![
        roster,
        operator,
        entry(19, "The pool pane now follows the log."),
        proposal,
        vote.clone(),
        report,
    ];
    if mode == "active" {
        vote.seq = 24;
        vote.author_agent_id = Some(MEMBERS[2].parse().unwrap());
        vote.author_name = Some("Cy".into());
        vote.author_role = Some("review".into());
        vote.decision = Some(VoteDecision::Block);
        vote.body = "Narrow layout still clips criteria.".into();
        out.push(vote);
    } else {
        for (seq, i) in [(25, 1), (30, 2), (31, 3)] {
            if i == 3 && mode != "completed" {
                break;
            }
            let mut v = vote.clone();
            v.seq = seq;
            v.author_agent_id = Some(MEMBERS[i].parse().unwrap());
            v.author_name = Some(["Ada", "Ben", "Cy", "Eli"][i].into());
            v.author_role = Some(["implement", "verify", "review", "verify"][i].into());
            out.push(v);
        }
    }
    out
}
/// Socket-shaped page including the intentionally overlapping reverse next_cursor.
fn page_value(
    mode: &str,
    entries: Vec<PoolEntryView>,
    before: Option<u64>,
    complete: bool,
) -> Value {
    let next = entries.last().map(|e| e.seq);
    json!({"pool_id":ID,"entries":entries,"before_seq":before,"after_seq":if before.is_none() {Some(0)} else {None},
        "last_seq":if mode == "empty" {Value::Null} else {json!(31)},"complete":complete,"next_cursor":next,"limit":50,"status":status(mode)})
}
/// Deserializes the public read envelope.
fn page(mode: &str, entries: Vec<PoolEntryView>, before: Option<u64>, complete: bool) -> Page {
    serde_json::from_value(page_value(mode, entries, before, complete)).unwrap()
}
/// Builds one compact discovery page with exact totals and state filtering.
fn list(mode: &str, offset: usize, total: u64) -> ListPoolsView {
    let s: PoolStatus = serde_json::from_value(status(mode)).unwrap();
    serde_json::from_value(json!({"items":[{"pool_id":ID,"state":s.state,"goal":s.goal,"goal_truncated":false,
        "created_at":1.0,"last_seq":31,"completed_at":Value::Null,"roster_revision":2,
        "members_count":4,"ready":s.ready(),"current_proposal_seq":20,
        "members":s.members.iter().map(|m|json!({"agent_id":m.agent_id,"slot":m.slot,"name":m.name,"role":m.role,"tip_status":m.tip_status})).collect::<Vec<_>>() }],
        "total":total,"offset":offset,"limit":50,"next_offset":if total > offset as u64 + 50 {Some(offset+50)} else {None},
        "complete":total <= offset as u64 + 50})).unwrap()
}
/// Empty filtered discovery page.
fn empty_list(offset: usize) -> ListPoolsView {
    serde_json::from_value(
        json!({"items":[],"total":0,"offset":offset,"limit":50,"next_offset":null,"complete":true}),
    )
    .unwrap()
}
/// Pool-tab app with matching current session metadata; completed joins intentionally show latest.
fn fixture(mode: &str, width: u16, height: u16) -> App {
    force_truecolor();
    let mut app = App::new();
    app.loaded = true;
    app.link = crate::app::Link::Up;
    app.revision = Some(42);
    app.last_width = width;
    app.last_height = height;
    app.pools.visible = true;
    app.pools.focused = true;
    app.pools.listing(
        0,
        if mode == "completed" {
            empty_list(0)
        } else {
            list(mode, 0, 1)
        },
        if mode == "completed" {
            list(mode, 0, 1)
        } else {
            empty_list(0)
        },
    );
    for (at, id) in MEMBERS.iter().enumerate() {
        let mut agent = agent_view(
            id,
            id,
            if mode == "completed" || mode == "waiting" && at < 3 {
                "succeeded"
            } else {
                "running"
            },
        );
        agent.runtime = ["codex", "claude", "glm", "codex"][at].into();
        agent.model = ["native-a", "native-b", "native-c", "native-a"][at].into();
        app.sessions.push(agent);
    }
    let pane_width = width.saturating_sub(if width >= 110 { 47 } else { 3 });
    app.pools.buffer_mut().unwrap().merge(
        page(
            mode,
            if mode == "empty" {
                vec![]
            } else {
                entries(mode)
            },
            Some(i64::MAX as u64),
            true,
        ),
        usize::from(pane_width),
    );
    app
}
/// Exact terminal cell dump, preserving spaces and final newline for golden comparisons.
fn frame(app: &App, width: u16, height: u16) -> String {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal.draw(|f| crate::ui::render(f, app)).unwrap();
    let buffer = terminal.backend().buffer();
    let mut out = String::new();
    for y in 0..height {
        for x in 0..width {
            out.push_str(buffer[(x, y)].symbol());
        }
        out.push('\n');
    }
    out
}
/// Compares a deterministic frame with committed data; opt-in update writes only crate goldens.
fn golden(mode: &str, width: u16, height: u16) {
    let app = fixture(mode, width, height);
    let actual = frame(&app, width, height);
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("tests/golden/pools-{mode}-{width}x{height}.txt"));
    if std::env::var_os("UPDATE_POOL_GOLDENS").is_some() {
        std::fs::write(&path, &actual).unwrap();
    }
    let expected = std::fs::read_to_string(&path).unwrap();
    assert_eq!(actual, expected, "golden {}", path.display());
    assert_eq!(actual.lines().count(), height as usize);
    assert!(
        actual
            .lines()
            .all(|row| ratatui::text::Line::from(row).width() == width as usize)
    );
    assert!(actual.contains("CHAT · untrusted"));
    if mode != "empty" {
        assert!(actual.contains("PROPOSAL"));
        assert!(actual.contains("snapshot (#20; untrusted)"));
    }
    assert!(actual.contains("roster r2"));
    if mode == "completed" {
        assert!(actual.contains("frozen broker status"));
        assert!(actual.contains("Formal checks only"));
    }
}

/// Replays a caller-owned response captured before first messages; the private fixture is never committed.
#[tokio::test]
#[ignore = "requires SAVED_TUI_POOL_RESPONSE"]
async fn saved_empty_pool_response_replays_through_decoder() {
    let path = std::env::var("SAVED_TUI_POOL_RESPONSE").expect("saved response");
    let value: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert!(
        value["last_seq"].is_null(),
        "fixture must preserve the original empty log"
    );
    let id: PoolId = serde_json::from_value(value["pool_id"].clone()).unwrap();
    let (broker, _calls) = FakeBroker::scripted(vec![Ok(value)]);
    let request = Request {
        visible: true,
        id: Some(id.clone()),
        ..Request::default()
    };
    let page = read(&*broker, &request).await.unwrap();
    assert!(page.last_seq.is_none());
    let mut app = App::new();
    app.pools.visible = true;
    app.pools.focused = true;
    app.pools.select(id);
    app.pools.buffer_mut().unwrap().merge(page, 77);
    let rendered = frame(&app, 80, 24);
    assert!(!rendered.contains("Opening pool"));
    assert!(rendered.contains("OPEN"));
    assert!(rendered.contains("seq —"));
    println!("captured empty broker response: decoded and rendered without invented watermark");
}

/// Opt-in read-only smoke against a caller-selected live broker and pool; never starts model turns.
#[tokio::test]
#[ignore = "requires LIVE_TUI_SOCKET and LIVE_TUI_POOL"]
async fn live_pool_read_only_smoke() {
    let socket = std::env::var("LIVE_TUI_SOCKET").expect("live socket");
    let id: PoolId = std::env::var("LIVE_TUI_POOL")
        .expect("live pool")
        .parse()
        .unwrap();
    let broker = crate::net::SocketBroker::new(socket);
    let mut app = App::new();
    app.pools.visible = true;
    app.pools.focused = true;
    app.pools.select(id);
    let request = app.pools.request(&[]);
    let page = tokio::time::timeout(Duration::from_secs(5), read(&broker, &request))
        .await
        .unwrap()
        .unwrap();
    let state = page.status.state;
    let absent = page.last_seq.is_none();
    app.pools.buffer_mut().unwrap().merge(page, 77);
    for (width, height) in [(80, 24), (120, 36), (180, 45)] {
        app.last_width = width;
        app.last_height = height;
        let rendered = frame(&app, width, height);
        assert!(!rendered.contains("Opening pool"));
        assert!(!rendered.contains("invalid type"));
        assert!(rendered.contains("OPEN") || rendered.contains("COMPLETED"));
    }
    println!(
        "live read/render: state={}, absent_watermark={}, three terminal sizes",
        state.as_str(),
        absent
    );
}

/// Empty public responses retain the absent durable watermark and poll from cursor zero.
#[tokio::test]
async fn empty_pool_null_watermark_opens_and_polls() {
    let value = page_value("empty", vec![], Some(i64::MAX as u64), true);
    let (broker, mut calls) = FakeBroker::scripted(vec![Ok(value.clone()), Ok(value)]);
    let mut app = App::new();
    app.pools.visible = true;
    app.pools.select(ID.parse().unwrap());
    let request = app.pools.request(&[]);
    let page = read(&*broker, &request).await.unwrap();
    assert_eq!(page.last_seq, None);
    assert_eq!(page.status.current_proposal, None);
    let b = app.pools.buffer_mut().unwrap();
    assert!(b.merge(page, 77));
    assert_eq!(b.last_seq, None);
    assert_eq!(b.after, 0);
    assert_eq!(b.cursor, None);
    assert!(b.error.is_none());
    assert_eq!(calls.recv().await.unwrap()["before_seq"], i64::MAX);
    let request = app.pools.request(&[]);
    assert!(request.initialized);
    assert!(!request.completed);
    let page = read(&*broker, &request).await.unwrap();
    assert!(!app.pools.buffer_mut().unwrap().merge(page, 77));
    assert_eq!(calls.recv().await.unwrap()["after_seq"], 0);
}

/// Two pool read lanes with independently gated first calls and recorded method/target receipts.
struct PoolLanes {
    /// Delay the first discovery request until explicitly released.
    list_delay: std::sync::atomic::AtomicBool,
    /// Delay detail reads of the original pool, allowing a new selection to proceed immediately.
    detail_delay: bool,
    /// Admission receipts for discovery and detail reads respectively.
    entered: [tokio::sync::Notify; 2],
    /// Release gates for discovery and detail reads respectively.
    release: [tokio::sync::Notify; 2],
    /// Exact admitted read methods and pool identities; no payload content is recorded.
    calls: Mutex<Vec<(String, Option<String>)>>,
}
impl PoolLanes {
    /// Builds a synthetic broker with optional delay on each independent read lane.
    fn new(list: bool, detail: bool) -> Arc<Self> {
        Arc::new(Self {
            list_delay: std::sync::atomic::AtomicBool::new(list),
            detail_delay: detail,
            entered: std::array::from_fn(|_| tokio::sync::Notify::new()),
            release: std::array::from_fn(|_| tokio::sync::Notify::new()),
            calls: Mutex::new(vec![]),
        })
    }
}
impl crate::net::Broker for PoolLanes {
    /// Records actual calls and serves two selectable pool summaries plus empty public detail pages.
    fn call<'a>(&'a self, method: &'a str, params: Value) -> crate::net::BrokerFuture<'a> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push((method.into(), params["pool_id"].as_str().map(str::to_owned)));
            if method == "list_pools" {
                self.entered[0].notify_one();
                if self.list_delay.swap(false, Ordering::Relaxed) {
                    self.release[0].notified().await;
                }
                if params["state"] == "completed" {
                    return Ok(serde_json::to_value(empty_list(0)).unwrap());
                }
                let mut page = list("active", 0, 2);
                let mut second = page.items[0].clone();
                second.pool_id = "pool-20261006-000000-aaaaaaaaaa".parse().unwrap();
                page.items.push(second);
                return Ok(serde_json::to_value(page).unwrap());
            }
            self.entered[1].notify_one();
            if self.detail_delay && params["pool_id"] == ID {
                self.release[1].notified().await;
            }
            let mut page = page_value("empty", vec![], Some(i64::MAX as u64), true);
            page["pool_id"] = params["pool_id"].clone();
            Ok(page)
        })
    }
}

/// Pool tab's first listing auto-selects and loads detail, arrows retarget it, and refresh preserves the cache.
#[tokio::test]
async fn tab_default_arrow_and_refresh_load_without_click() {
    let broker = PoolLanes::new(false, false);
    let mut app = App::new();
    events::apply_action(&mut app, Global::Tab(true));
    let (requests, rx) = watch::channel(app.pools.request(&[]));
    let (tx, mut events_rx) = mpsc::channel(8);
    let task = tokio::spawn(worker(broker.clone(), rx, tx));
    let mut pipeline = Pipeline::new(events::FRAME_INTERVAL);
    let listing = tokio::time::timeout(Duration::from_millis(300), events_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(listing, BrokerEvent::Pools { .. }));
    pipeline.broker_event(&mut app, listing);
    assert!(app.pools.selected.is_some());
    requests.send(app.pools.request(&[])).unwrap();
    let detail = tokio::time::timeout(Duration::from_millis(300), events_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(detail, BrokerEvent::Pool { .. }));
    pipeline.broker_event(&mut app, detail);
    assert!(app.pools.buffer().unwrap().status.is_some());
    pipeline.ui_event(
        &mut app,
        UiEvent::Terminal(Event::Key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE))),
    );
    let selected = app.pools.selected.clone().unwrap();
    assert_ne!(selected.as_str(), ID);
    requests.send(app.pools.request(&[])).unwrap();
    let detail = tokio::time::timeout(Duration::from_millis(300), events_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(&detail,BrokerEvent::Pool {id,..} if id == &selected));
    pipeline.broker_event(&mut app, detail);
    let cached = app.pools.buffer().unwrap().status.clone();
    let old_refresh = app.pools.refresh;
    pipeline.ui_event(
        &mut app,
        UiEvent::Terminal(Event::Key(KeyEvent::new(
            KeyCode::Char('r'),
            KeyModifiers::NONE,
        ))),
    );
    assert_eq!(app.pools.refresh, old_refresh + 1);
    assert_eq!(app.pools.buffer().unwrap().status, cached);
    requests.send(app.pools.request(&[])).unwrap();
    for _ in 0..2 {
        pipeline.broker_event(
            &mut app,
            tokio::time::timeout(Duration::from_millis(300), events_rx.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let calls = broker.calls.lock().unwrap().len();
    for _ in 0..20 {
        pipeline.prepare_frame(&mut app);
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        broker.calls.lock().unwrap().len(),
        calls,
        "unchanged frames do not dispatch new fetches"
    );
    drop(requests);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
}

/// A delayed discovery never blocks selected detail, and a delayed old detail never starves discovery/new selection.
#[tokio::test]
async fn discovery_and_detail_progress_independently_and_latest_selection_wins() {
    for (delay_list, delay_detail) in [(true, false), (false, true)] {
        let broker = PoolLanes::new(delay_list, delay_detail);
        let mut request = Request {
            visible: true,
            id: Some(ID.parse().unwrap()),
            ..Request::default()
        };
        let (requests, rx) = watch::channel(request.clone());
        let (tx, mut events_rx) = mpsc::channel(8);
        let task = tokio::spawn(worker(broker.clone(), rx, tx));
        tokio::time::timeout(
            Duration::from_millis(300),
            broker.entered[usize::from(delay_detail)].notified(),
        )
        .await
        .unwrap();
        let event = tokio::time::timeout(Duration::from_millis(300), events_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(if delay_list {
            matches!(event, BrokerEvent::Pool { .. })
        } else {
            matches!(event, BrokerEvent::Pools { .. })
        });
        if delay_detail {
            request.id = Some("pool-20261006-000000-aaaaaaaaaa".parse().unwrap());
            requests.send(request.clone()).unwrap();
            let event = tokio::time::timeout(Duration::from_millis(300), events_rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(&event,BrokerEvent::Pool {id,..} if Some(id) == request.id.as_ref()));
        } else {
            broker.release[0].notify_one();
            assert!(matches!(
                tokio::time::timeout(Duration::from_millis(300), events_rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                BrokerEvent::Pools { .. }
            ));
        }
        drop(requests);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }
}

/// Hovering a different pool selects and requests it without any click or Enter.
#[test]
fn pool_hover_selects_without_click() {
    let mut app = fixture("active", 120, 36);
    app.pools.focused = false;
    let other: PoolId = "pool-20261006-000000-aaaaaaaaaa".parse().unwrap();
    let mut item = app.pools.items[0].clone();
    item.pool_id = other.clone();
    app.pools.items.push(item);
    frame(&app, 120, 36);
    let hit = app
        .pools
        .hits
        .borrow()
        .iter()
        .find_map(|(r, target)| matches!(target,Target::Pool(id) if id == &other).then_some(*r))
        .unwrap();
    let mut pipeline = crate::events::Pipeline::new(crate::events::FRAME_INTERVAL);
    pipeline.ui_event(
        &mut app,
        crate::events::UiEvent::Terminal(ratatui::crossterm::event::Event::Mouse(MouseEvent {
            kind: MouseEventKind::Moved,
            column: hit.x,
            row: hit.y,
            modifiers: KeyModifiers::NONE,
        })),
    );
    pipeline.prepare_frame(&mut app);
    assert_eq!(app.pools.selected, Some(other.clone()));
    assert_eq!(app.pools.request(&app.sessions).id, Some(other));
    assert!(!app.pools.focused);
}

/// Quiet completed pools ignore unrelated activity; visible first loads animate and cached views do not.
#[test]
fn pool_animation_tracks_visible_content_and_cache() {
    let mut app = fixture("completed", 80, 24);
    app.sessions[0].status = Status::Running;
    app.dirty = false;
    let before = frame(&app, 80, 24);
    for _ in 0..10 {
        app.tick();
        assert!(!app.dirty);
    }
    assert_eq!(
        before,
        frame(&app, 80, 24),
        "no stale aggregate spinner changes on input"
    );
    app.pools.buffer_mut().unwrap().status = None;
    assert!(app.tick_animates());
    let before = frame(&app, 80, 24);
    app.tick();
    assert!(app.dirty);
    assert_ne!(
        before,
        frame(&app, 80, 24),
        "visible first load advances without input"
    );
    app.pools.focused = false;
    app.dirty = false;
    app.tick();
    assert!(!app.dirty, "hidden opening pane does not animate the list");
}

/// Empty pools remain usable at narrow, medium and wide operator sizes.
#[test]
fn golden_empty_operator_sizes() {
    for (width, height) in [(80, 24), (120, 36), (180, 45)] {
        golden("empty", width, height);
        let mut app = fixture("empty", width, height);
        let status = app.pools.buffer_mut().unwrap().status.as_mut().unwrap();
        status.goal = "Long sanitized goal ".repeat(100);
        status.members[0].role = status.members[0].name.clone();
        let rendered = frame(&app, width, height);
        assert!(rendered.contains("CHAT · untrusted"));
        assert!(rendered.contains("proposal —"));
        assert!(!rendered.contains("Ada (Ada)"));
        assert!(!rendered.contains("Criteria  "));
        assert!(!rendered.contains("c full goal"));
        apply(&mut app, Action::Criteria);
        apply(&mut app, Action::Bottom);
        assert!(frame(&app, width, height).contains("Criteria (untrusted)"));
        apply(&mut app, Action::Back);
        apply(&mut app, Action::Roster);
        apply(&mut app, Action::History);
        assert!(app.pools.buffer().unwrap().history);
        assert!(app.pools.buffer().unwrap().roster);
    }
}

/// Active wide frame from the design.
#[test]
fn golden_active_160x48() {
    golden("active", 160, 48);
}
/// Active narrow detail frame from the design.
#[test]
fn golden_active_66x52() {
    golden("active", 66, 52);
}
/// Waiting on the last valid vote, wide.
#[test]
fn golden_waiting_160x48() {
    golden("waiting", 160, 48);
}
/// Waiting on the last valid vote, narrow.
#[test]
fn golden_waiting_66x52() {
    golden("waiting", 66, 52);
}
/// Completed frozen pool, wide.
#[test]
fn golden_completed_160x48() {
    golden("completed", 160, 48);
}
/// Completed frozen pool, narrow.
#[test]
fn golden_completed_66x52() {
    golden("completed", 66, 52);
}

/// Raw ready votes with stale reasons never inflate readiness; completed proof stays frozen.
#[test]
fn validity_status_changes_and_completion_freeze() {
    let mut b = Buffer::new(ID.parse().unwrap());
    let mut p = page("active", vec![], None, true);
    p.status.members[1].vote = Some(Vote {
        decision: VoteDecision::Ready,
    });
    p.status.members[1].why = "stale_roster".into();
    assert!(b.merge(p.clone(), 113));
    assert_eq!(b.status.as_ref().unwrap().ready(), 1);
    assert!(!b.merge(p, 113), "unchanged poll must not draw");
    assert!(b.merge(page("waiting", vec![], None, true), 113));
    assert_eq!(b.status.as_ref().unwrap().ready(), 3);
    assert!(b.merge(page("completed", vec![], None, true), 113));
    let frozen = b.status.clone();
    b.error = Some("old failure".into());
    b.older = Some(20);
    assert!(b.merge(page("completed", vec![], Some(20), true), 113));
    assert!(b.error.is_none() && b.older.is_none());
    assert!(!b.merge(page("active", vec![], None, true), 113));
    assert_eq!(b.status, frozen);
}
/// Reverse overlap merges once, minimum returned seq becomes the cursor, and anchors survive.
#[test]
fn forward_backward_overlap_preserves_anchor_and_cursor() {
    let mut app = fixture("active", 160, 48);
    let b = app.pools.buffer_mut().unwrap();
    b.follow = false;
    b.offset = 3;
    let anchor = crate::ui::pools::rows(b, 113)[b.offset].seq;
    assert!(b.merge(
        page(
            "active",
            vec![entry(8, "older"), entry(10, "immutable overlap")],
            Some(18),
            false
        ),
        113
    ));
    assert_eq!(b.entries.first().unwrap().seq, 8);
    assert_eq!(b.after, 24);
    assert_eq!(crate::ui::pools::rows(b, 113)[b.offset].seq, anchor);
    b.offset = 0;
    older(&mut app);
    assert_eq!(
        app.pools.request(&app.sessions).before,
        Some(8),
        "never use reverse next_cursor #10"
    );
    let b = app.pools.buffer_mut().unwrap();
    assert!(b.merge(
        page(
            "active",
            vec![entry(24, "immutable overlap"), entry(32, "new")],
            None,
            true
        ),
        113
    ));
    assert_eq!(b.entries.iter().filter(|e| e.seq == 24).count(), 1);
    assert_eq!(b.after, 32);
}

/// A nearly full scrolled window admits only retained bodies, preserving its older anchor.
#[test]
fn scrolled_forward_page_stops_at_remaining_capacity() {
    let mut buffer = Buffer::new(ID.parse().unwrap());
    buffer.merge(
        page(
            "active",
            (1..500).map(|seq| entry(seq, "body")).collect(),
            None,
            true,
        ),
        63,
    );
    buffer.follow = false;
    buffer.offset = 1;
    buffer.cursor = Some(1);
    let rows = crate::ui::pools::rows(&buffer, 63);
    let anchor = rows[buffer.offset].seq;
    let incoming = page(
        "active",
        (500..550).map(|seq| entry(seq, "incoming")).collect(),
        None,
        true,
    );
    assert!(buffer.merge(incoming.clone(), 63));
    assert_eq!(buffer.entries.len(), ENTRY_CAP);
    assert_eq!(buffer.entries.first().unwrap().seq, 1);
    assert_eq!(buffer.entries.last().unwrap().seq, 500);
    assert_eq!(buffer.after, 500);
    assert_eq!(buffer.cursor, Some(1));
    assert_eq!(
        crate::ui::pools::rows(&buffer, 63)[buffer.offset].seq,
        anchor
    );
    assert!(!buffer.merge(incoming.clone(), 63));
    assert_eq!(buffer.after, 500);
    buffer.follow = true;
    buffer.merge(incoming, 63);
    assert_eq!(buffer.after, 549);
    assert_eq!(buffer.entries.first().unwrap().seq, 50);
    assert_eq!(buffer.entries.last().unwrap().seq, 549);
}

/// End persists the real last page; reverse navigation immediately moves and overscroll clamps.
#[test]
fn criteria_end_then_up_moves_one_wrapped_line() {
    let mut app = fixture("active", 66, 20);
    app.pools
        .buffer_mut()
        .unwrap()
        .status
        .as_mut()
        .unwrap()
        .goal = (0..100)
        .map(|n| format!("goal line {n}"))
        .collect::<Vec<_>>()
        .join("\n");
    apply(&mut app, Action::Criteria);
    let max = crate::ui::pools::criteria_max_offset(&app);
    assert!(max > 10);
    apply(&mut app, Action::Bottom);
    assert_eq!(app.pools.criteria_offset, max);
    let bottom = frame(&app, 66, 20);
    apply(&mut app, Action::Move(-1));
    assert_eq!(app.pools.criteria_offset, max - 1);
    assert_ne!(frame(&app, 66, 20), bottom);
    apply(&mut app, Action::Scroll(i64::MAX));
    assert_eq!(app.pools.criteria_offset, max);
    apply(&mut app, Action::Scroll(-10));
    assert_eq!(app.pools.criteria_offset, max - 10);
    app.last_height = 40;
    let resized_max = crate::ui::pools::criteria_max_offset(&app);
    assert!(resized_max < max);
    app.pools.criteria_offset = usize::MAX;
    apply(&mut app, Action::Move(-1));
    assert_eq!(app.pools.criteria_offset, resized_max.saturating_sub(1));
}

/// Retained windows and MRU stay bounded; older pages can evict tail without losing the reader.
#[test]
fn memory_caps_and_pool_restore() {
    let mut pools = Pools::default();
    pools.select(ID.parse().unwrap());
    let b = pools.buffer_mut().unwrap();
    b.merge(
        page(
            "active",
            (1..=600).map(|seq| entry(seq, "body")).collect(),
            None,
            true,
        ),
        63,
    );
    assert_eq!(b.entries.len(), ENTRY_CAP);
    assert_eq!(b.entries.first().unwrap().seq, 101);
    assert!(!b.history_complete);
    b.follow = false;
    b.offset = 4;
    let anchor = crate::ui::pools::rows(b, 63)[4].seq;
    b.merge(
        page(
            "active",
            (51..=100).map(|seq| entry(seq, "older")).collect(),
            Some(101),
            false,
        ),
        63,
    );
    assert_eq!(crate::ui::pools::rows(b, 63)[b.offset].seq, anchor);
    let before = b.after;
    b.merge(
        page(
            "active",
            vec![entry(601, "incoming while reading older")],
            None,
            true,
        ),
        63,
    );
    assert_eq!(
        b.after, before,
        "bounded scrolled window retains its older edge"
    );
    assert_eq!(b.entries.last().unwrap().seq, 550);
    let id: PoolId = ID.parse().unwrap();
    for i in 1..4 {
        pools.select(format!("pool-20261004-100000-{i:010x}").parse().unwrap());
    }
    pools.select(id.clone());
    assert!(!pools.buffer().unwrap().follow);
    assert_eq!(pools.buffers.len(), 4);
    pools.select("pool-20261004-100000-fffffffffe".parse().unwrap());
    assert_eq!(pools.buffers.len(), 4);
}
/// Discovery uses exact totals, rejects stale offsets and keeps selected pool IDs through reordering.
#[tokio::test]
async fn fake_broker_list_paging_and_unchanged_draw() {
    let first = list("active", 0, 120);
    let second = list("waiting", 50, 120);
    let (broker, mut requests) = FakeBroker::scripted(vec![
        Ok(serde_json::to_value(&first).unwrap()),
        Ok(serde_json::to_value(&second).unwrap()),
    ]);
    let mut app = App::new();
    let mut pipeline = Pipeline::new(Duration::ZERO);
    let first = listing(&*broker, PoolState::Open, 0).await.unwrap();
    assert_eq!(
        requests.recv().await.unwrap(),
        json!({"state":"open","limit":50,"offset":0})
    );
    pipeline.broker_event(
        &mut app,
        BrokerEvent::Pools {
            offset: 0,
            fallback: false,
            page: Ok((first.clone(), empty_list(0))),
        },
    );
    assert_eq!(app.pools.open_total, 120);
    app.dirty = false;
    pipeline.broker_event(
        &mut app,
        BrokerEvent::Pools {
            offset: 0,
            fallback: false,
            page: Ok((first, empty_list(0))),
        },
    );
    assert!(!app.dirty);
    app.pools.visible = true;
    apply(&mut app, Action::Page(1));
    assert_eq!(app.pools.offset, 50);
    let second = listing(&*broker, PoolState::Open, 50).await.unwrap();
    assert_eq!(requests.recv().await.unwrap()["offset"], 50);
    pipeline.broker_event(
        &mut app,
        BrokerEvent::Pools {
            offset: 50,
            fallback: false,
            page: Ok((second, empty_list(50))),
        },
    );
    assert_eq!(app.pools.selected.as_ref().unwrap().as_str(), ID);
    assert_eq!(app.pools.items[0].ready, 3);
    app.dirty = false;
    pipeline.broker_event(
        &mut app,
        BrokerEvent::Pools {
            offset: 0,
            fallback: false,
            page: Ok((list("active", 0, 2), empty_list(0))),
        },
    );
    assert!(!app.dirty);
    assert_eq!(app.pools.open_total, 120);
}
/// Fake reads exercise tail, forward and reverse wire cursors with immutable overlap.
#[tokio::test]
async fn fake_broker_read_cursors_and_overlap() {
    let (broker, mut requests) = FakeBroker::scripted(vec![
        Ok(page_value(
            "active",
            vec![entry(20, "tail"), entry(21, "tail")],
            Some(i64::MAX as u64),
            false,
        )),
        Ok(page_value(
            "waiting",
            vec![entry(21, "overlap"), entry(22, "forward")],
            None,
            true,
        )),
        Ok(page_value(
            "waiting",
            vec![entry(1, "old"), entry(20, "overlap")],
            Some(20),
            true,
        )),
    ]);
    let mut target = Request {
        visible: true,
        id: Some(ID.parse().unwrap()),
        ..Request::default()
    };
    let mut b = Buffer::new(ID.parse().unwrap());
    b.merge(read(&*broker, &target).await.unwrap(), 63);
    assert_eq!(requests.recv().await.unwrap()["before_seq"], i64::MAX);
    target.initialized = true;
    target.after = b.after;
    b.merge(read(&*broker, &target).await.unwrap(), 63);
    let params = requests.recv().await.unwrap();
    assert_eq!(params["after_seq"], 21);
    assert!(params.get("before_seq").is_none());
    target.before = Some(20);
    b.merge(read(&*broker, &target).await.unwrap(), 63);
    let params = requests.recv().await.unwrap();
    assert_eq!(params["before_seq"], 20);
    assert!(params.get("after_seq").is_none());
    assert_eq!(
        b.entries.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![1, 20, 21, 22]
    );
    assert_eq!(b.after, 22);
    assert!(b.history_complete);
}
/// Visible polling is throttled, hidden tabs cancel, and completed status stops forward reads.
#[tokio::test]
async fn worker_throttles_and_stops_when_hidden() {
    let (broker, mut calls) = FakeBroker::scripted(vec![
        Ok(serde_json::to_value(list("active", 0, 1)).unwrap()),
        Ok(serde_json::to_value(empty_list(0)).unwrap()),
        Ok(page_value(
            "completed",
            entries("completed"),
            Some(i64::MAX as u64),
            true,
        )),
    ]);
    let target = Request {
        visible: true,
        id: Some(ID.parse().unwrap()),
        ..Request::default()
    };
    let (tx, rx) = watch::channel(target.clone());
    let (events, mut delivered) = mpsc::channel(8);
    let task = tokio::spawn(worker(broker, rx, events));
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(2), calls.recv())
            .await
            .unwrap()
            .unwrap();
    }
    assert!(matches!(
        delivered.recv().await,
        Some(BrokerEvent::Pools { .. })
    ));
    assert!(matches!(
        delivered.recv().await,
        Some(BrokerEvent::Pool { .. })
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(80), calls.recv())
            .await
            .is_err()
    );
    tx.send(Request {
        visible: false,
        ..target
    })
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(1100), calls.recv())
            .await
            .is_err()
    );
    task.abort();
}
/// Key and mouse tab switches preserve Tab's existing finished toggle.
#[test]
fn tabs_keys_mouse_and_narrow_back() {
    let mut app = fixture("active", 66, 52);
    let mut pipeline = Pipeline::new(Duration::ZERO);
    let key = |code| KeyEvent::new(code, KeyModifiers::NONE);
    assert_eq!(
        events::key_action(&app, key(KeyCode::Char('1'))),
        Global::Tab(false)
    );
    events::apply_action(&mut app, Global::Tab(false));
    assert_eq!(
        events::key_action(&app, key(KeyCode::Tab)),
        Global::ToggleCompleted
    );
    assert_eq!(
        events::key_action(&app, key(KeyCode::BackTab)),
        Global::Tab(true)
    );
    pipeline.ui_event(
        &mut app,
        UiEvent::Terminal(Event::Mouse(MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 15,
            row: 1,
            modifiers: KeyModifiers::NONE,
        })),
    );
    assert!(app.pools.visible);
    apply(&mut app, Action::Back);
    assert!(!app.pools.focused);
    assert!(crate::ui::panes(&app, 66, 52).list.is_some());
    apply(&mut app, Action::Open);
    assert!(crate::ui::panes(&app, 66, 52).transcript.is_some());
    assert!(!frame(&app, 66, 52).contains("LIVE"));
}
/// Member and retired-member transcripts reuse the existing view and restore pool navigation.
#[test]
fn member_transcript_back_and_cache() {
    let mut app = fixture("active", 160, 48);
    apply(&mut app, Action::Roster);
    app.pools.buffer_mut().unwrap().follow = false;
    app.pools.buffer_mut().unwrap().offset = 7;
    assert!(matches!(
        apply(&mut app, Action::Open),
        Dispatched::PoolMember(_)
    ));
    let mut pipeline = Pipeline::new(Duration::ZERO);
    pipeline.broker_event(
        &mut app,
        BrokerEvent::PoolMember(Ok(Box::new(agent_view(MEMBERS[0], MEMBERS[0], "running")))),
    );
    assert!(app.pools.member_transcript);
    assert!(matches!(
        pipeline.prepare_frame(&mut app),
        Dispatched::Watch(..)
    ));
    assert!(
        crate::ui::panes(&app, 160, 48).list.is_none(),
        "member transcript gets full width"
    );
    assert!(matches!(
        events::apply_action(&mut app, Global::Back),
        Dispatched::Clear
    ));
    assert!(app.pools.focused && !app.pools.member_transcript);
    assert_eq!(app.pools.buffer().unwrap().offset, 7);
    assert!(!app.pools.buffer().unwrap().follow);
    apply(&mut app, Action::History);
    apply(&mut app, Action::Move(4));
    assert_eq!(
        app.pools.buffer().unwrap().member_id().unwrap().as_str(),
        RETIRED
    );
    apply(&mut app, Action::Transcript);
    pipeline.broker_event(
        &mut app,
        BrokerEvent::PoolMember(Ok(Box::new(agent_view(RETIRED, RETIRED, "failed")))),
    );
    assert!(app.pools.member_transcript);
    assert_eq!(
        app.transcript.as_ref().unwrap().agent.agent_id.as_str(),
        RETIRED
    );
}
/// A cancelled member request cannot navigate after Esc.
#[test]
fn cancelled_member_response_is_ignored() {
    let mut app = fixture("active", 66, 52);
    apply(&mut app, Action::Transcript);
    apply(&mut app, Action::Back);
    open_member(&mut app, agent_view(MEMBERS[0], MEMBERS[0], "running"));
    assert!(!app.pools.member_transcript);
}
/// All untrusted surfaces remove controls; unavailable historical snapshots remain explicit.
#[test]
fn sanitization_expansion_snapshot_identity_and_memo_reuse() {
    let mut app = fixture("active", 66, 52);
    let b = app.pools.buffer_mut().unwrap();
    b.follow = false;
    b.offset = 0;
    let mut proposal = entry(1, "\x1b[31mred\x1b[0m\x07");
    proposal.kind = EntryKind::Proposal;
    b.merge(page("active", vec![proposal], Some(10), true), 63);
    let mut revoke = entry(32, "vote withdrawn\x1b]0;hidden\x07");
    revoke.kind = EntryKind::Revoke;
    revoke.proposal_seq = Some(20);
    b.merge(page("active", vec![revoke], None, true), 63);
    b.expanded.insert(32);
    let rows = crate::ui::pools::rows(b, 63);
    let again = crate::ui::pools::rows(b, 63);
    assert!(
        Arc::ptr_eq(&rows, &again),
        "unchanged blocks reuse the flat index"
    );
    let text = rows
        .iter()
        .map(|r| r.line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!text.contains('\x1b') && !text.contains('\x07'));
    assert!(text.contains("snapshot unavailable"));
    assert!(text.contains("  red"));
    assert!(text.contains("revoke #20"));
    assert!(text.contains("  vote withdrawn"));
    assert_eq!(
        text.matches("Sessions + pool log").count(),
        1,
        "current snapshot belongs only to #20"
    );
    apply(&mut app, Action::Criteria);
    assert!(frame(&app, 66, 52).contains("goal + criteria"));
    apply(&mut app, Action::Back);
    assert!(!app.pools.criteria);
}

/// Pool pointer moves are consumed without scheduling an unchanged frame forever.
#[test]
fn unchanged_pool_pointer_stays_idle() {
    let mut app = fixture("completed", 160, 48);
    let mut pipeline = Pipeline::new(Duration::ZERO);
    app.dirty = false;
    pipeline.ui_event(
        &mut app,
        UiEvent::Terminal(Event::Mouse(MouseEvent {
            kind: MouseEventKind::Moved,
            column: 70,
            row: 25,
            modifiers: KeyModifiers::NONE,
        })),
    );
    assert!(
        pipeline
            .next_frame(&app, std::time::Instant::now())
            .is_some()
    );
    pipeline.prepare_frame(&mut app);
    assert!(
        pipeline
            .next_frame(&app, std::time::Instant::now())
            .is_none()
    );
}
/// Wheel routing uses the pointer's pane even when keyboard focus is in the other pane.
#[test]
fn pool_wheel_uses_pointed_pane() {
    let mut app = fixture("active", 160, 48);
    let _ = frame(&app, 160, 48);
    let mut pipeline = Pipeline::new(Duration::ZERO);
    app.pools.focused = false;
    pipeline.ui_event(
        &mut app,
        UiEvent::Terminal(Event::Mouse(MouseEvent {
            kind: MouseEventKind::ScrollUp,
            column: 70,
            row: 25,
            modifiers: KeyModifiers::NONE,
        })),
    );
    pipeline.prepare_frame(&mut app);
    assert!(app.pools.focused);
    assert!(!app.pools.buffer().unwrap().follow);
    pipeline.ui_event(
        &mut app,
        UiEvent::Terminal(Event::Mouse(MouseEvent {
            kind: MouseEventKind::ScrollDown,
            column: 10,
            row: 5,
            modifiers: KeyModifiers::NONE,
        })),
    );
    pipeline.prepare_frame(&mut app);
    assert!(!app.pools.focused);
}
