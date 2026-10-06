//! Synthetic full-history copy tests; never reads or changes the user's real clipboard.
use super::*;
use crate::tests_support::{FakeBroker, agent_view};
use agent_run_domain::pool::{AuthorKind, Direction, EntryKind, PoolEntryView};
use std::sync::Mutex;

/// Stable illustrative agent used only in fixtures.
const AGENT: &str = "ag-20260928-101500-aaaaaaaaaa";
/// Illustrative pool unrelated to private live pools.
const POOL: &str = "pool-20261004-100000-7ac03b9e12";

/// Authoritative bounded tail response at the supplied sequence.
fn tail(seq: i64) -> Value {
    json!({"agent_id":AGENT,"messages":if seq == 0 {vec![]} else {vec![json!({
        "seq":seq,"at":1.0,"role":"assistant","name":null,"content":"tail","raw_ref":"item",
        "content_complete":true,"last_seq":seq
    })]},"cursor":0,"limit":1,"next_cursor":null,"complete":true,"view":"blocks",
        "direction":"backward","resume_cursor":if seq == 0 {None} else {Some(seq)}})
}
/// One complete raw journal row, retaining native tool-result error facts and whitespace.
fn message(seq: i64, role: &str, content: &str) -> Value {
    json!({"seq":seq,"at":seq as f64,"role":role,"name":if role.starts_with("tool") {Some("shell")} else {None},
        "content":content,"raw_ref":"item","content_complete":true,"starts_block":true,
        "error":if role == "tool_result" {Some(true)} else {None},
        "error_source":if role == "tool_result" {Some("native")} else {None}})
}
/// One raw page, including a truthful final flag and continuation cursor.
fn page(messages: Vec<Value>, cursor: i64, complete: bool) -> Value {
    let next = messages.last().and_then(|m| m["seq"].as_i64());
    json!({"agent_id":AGENT,"messages":messages,"cursor":cursor,"limit":1000,
        "next_cursor":if complete {None} else {next},"complete":complete})
}
/// Viewer scope fixture; absence of run_id deliberately preserves retained lineage semantics.
fn target() -> Target {
    Target::Agent {
        agent: AGENT.parse().unwrap(),
        run: None,
    }
}

/// Atomic writer stub recording only successful complete payloads.
#[derive(Default)]
struct RecordingClipboard {
    /// Completed writes, empty when export failed or was cancelled.
    written: Mutex<Vec<String>>,
}
impl Clipboard for RecordingClipboard {
    /// Records one entire synthetic payload after complete export; rejects signalled cancellation.
    fn write<'a>(&'a self, text: String, cancel: watch::Receiver<bool>) -> ClipboardFuture<'a> {
        Box::pin(async move {
            if *cancel.borrow() {
                return Err("Copy cancelled".into());
            }
            self.written.lock().unwrap().push(text);
            Ok(())
        })
    }
}

/// Multiple pages include collapsed tools, Unicode, exact whitespace and error facts; appended tail is excluded.
#[tokio::test]
async fn multipage_copy_preserves_full_text_and_stops_at_snapshot() {
    let unicode = "Привет 🌍\n\t  trailing  \n";
    let (broker, mut calls) = FakeBroker::scripted(vec![
        Ok(tail(3)),
        Ok(page(
            vec![
                message(1, "user", unicode),
                message(2, "tool_call", "{\"cmd\":\"false\"}"),
            ],
            0,
            false,
        )),
        Ok(page(
            vec![
                message(
                    3,
                    "tool_result",
                    "\x1b[31mERROR\x1b[0m\n  full output  \n"
                        .repeat(20)
                        .as_str(),
                ),
                message(4, "assistant", "must not chase this new tail"),
            ],
            2,
            false,
        )),
    ]);
    let clipboard = RecordingClipboard::default();
    let (_cancel_tx, cancel) = watch::channel(false);
    let bytes = execute(
        &*broker,
        &clipboard,
        &Request {
            generation: 1,
            target: target(),
        },
        cancel,
    )
    .await
    .unwrap();
    let written = clipboard.written.lock().unwrap().clone();
    assert_eq!(written.len(), 1);
    let text = &written[0];
    assert_eq!(bytes, text.len());
    assert!(text.contains(unicode));
    assert!(text.contains("  full output  \n"));
    assert!(text.contains("tool_call · shell · id item"));
    assert!(text.contains("error=true"));
    assert!(text.contains("source=native"));
    assert!(!text.contains('\x1b'));
    assert!(!text.contains("must not chase"));
    assert!(!text.contains('…'));
    assert_eq!(calls.recv().await.unwrap()["tail_blocks"], 1);
    assert_eq!(calls.recv().await.unwrap()["cursor"], 0);
    assert_eq!(calls.recv().await.unwrap()["cursor"], 2);
}

/// A thousand streamed fragments across pages remain one uninterrupted logical message.
/// ANSI sequences split across fragments are removed, and final tool error evidence is retained.
#[tokio::test]
async fn streamed_fragments_remain_readable_across_pages() {
    let mut messages = (1..=1000)
        .map(|seq| {
            let mut m = message(seq, "assistant", "я");
            m["starts_block"] = json!(seq == 1);
            m
        })
        .collect::<Vec<_>>();
    messages[0]["content"] = json!(format!("he{}[", char::from(27)));
    messages[1]["content"] = json!(format!("31mllo{}[0m", char::from(27)));
    messages[999]["error"] = json!(true);
    messages[999]["error_source"] = json!("native_final");
    let second = messages.split_off(500);
    let (broker, _calls) = FakeBroker::scripted(vec![
        Ok(tail(1000)),
        Ok(page(messages, 0, false)),
        Ok(page(second, 500, true)),
    ]);
    let text = export(&*broker, &target()).await.unwrap();
    assert!(text.contains(&format!("hello{}", "я".repeat(998))));
    assert_eq!(text.matches("assistant · id item").count(), 1);
    assert!(text.contains("[tool status: error=true · source=native_final]"));
    assert!(!text.contains(char::from(27)));
    assert!(!text.contains("\\\"content\\\""));
}

/// Failed pages and false/unknown coverage never invoke the clipboard writer.
#[tokio::test]
async fn failed_or_unproven_history_keeps_clipboard_unchanged() {
    for coverage in [json!(false), Value::Null] {
        let mut incomplete = message(1, "assistant", "stored excerpt");
        incomplete["content_complete"] = coverage.clone();
        let (broker, _calls) =
            FakeBroker::scripted(vec![Ok(tail(1)), Ok(page(vec![incomplete], 0, true))]);
        let clipboard = RecordingClipboard::default();
        let (_cancel_tx, cancel) = watch::channel(false);
        let error = execute(
            &*broker,
            &clipboard,
            &Request {
                generation: 1,
                target: target(),
            },
            cancel,
        )
        .await
        .unwrap_err();
        assert!(error.contains(if coverage.is_null() {
            "unknown"
        } else {
            "spooled or incomplete"
        }));
        assert!(clipboard.written.lock().unwrap().is_empty());
    }
    let (broker, _calls) = FakeBroker::scripted(vec![
        Ok(tail(2)),
        Ok(page(vec![message(1, "user", "kept")], 0, false)),
        Err("fixture read failure".into()),
    ]);
    let clipboard = RecordingClipboard::default();
    let (_cancel_tx, cancel) = watch::channel(false);
    assert!(
        execute(
            &*broker,
            &clipboard,
            &Request {
                generation: 1,
                target: target()
            },
            cancel
        )
        .await
        .is_err()
    );
    assert!(clipboard.written.lock().unwrap().is_empty());
}

/// Size overflow refuses the copy instead of returning a prefix.
#[test]
fn copy_output_limit_never_truncates() {
    let mut text = "x".repeat(MAX_BYTES);
    assert!(append(&mut text, "overflow").is_err());
    assert_eq!(text.len(), MAX_BYTES);
}

/// Native stdin staging preserves Unicode and trailing whitespace without invoking the real clipboard.
#[cfg(target_os = "macos")]
#[test]
fn staged_clipboard_input_is_complete_and_unlinked() {
    use std::io::Read;
    let expected = "Привет 🌍\n\ttrailing  \n";
    let mut file = staged_input(expected).unwrap();
    let mut actual = String::new();
    file.read_to_string(&mut actual).unwrap();
    assert_eq!(actual, expected);
    use std::os::unix::fs::MetadataExt;
    assert_eq!(
        file.metadata().unwrap().nlink(),
        0,
        "no plaintext temporary path remains"
    );
}

/// Opt-in real broker export qualification with a memory clipboard sink; never invokes pbcopy.
/// Only byte count, watermark and an explicit coverage refusal are printed; private text is dropped.
#[tokio::test]
#[ignore = "requires LIVE_COPY_SOCKET and LIVE_COPY_TARGET"]
async fn live_export_to_memory_only() {
    let socket = std::env::var("LIVE_COPY_SOCKET").expect("live socket");
    let id = std::env::var("LIVE_COPY_TARGET").expect("live selection");
    let target = if id.starts_with("pool-") {
        Target::Pool(id.parse().unwrap())
    } else {
        Target::Agent {
            agent: id.parse().unwrap(),
            run: None,
        }
    };
    let broker = crate::net::SocketBroker::new(socket);
    let clipboard = RecordingClipboard::default();
    let (_cancel_tx, cancel) = watch::channel(false);
    let request = Request {
        generation: 1,
        target,
    };
    match execute(&broker, &clipboard, &request, cancel).await {
        Ok(bytes) => {
            let written = clipboard.written.lock().unwrap();
            assert_eq!(written.len(), 1);
            let boundary = written[0]
                .lines()
                .find(|line| line.starts_with("Snapshot through #"))
                .unwrap();
            println!("real export memory sink: complete, bytes={bytes}, {boundary}");
        }
        Err(error) if error.contains("spooled or incomplete") => {
            println!("real export refused: spooled/incomplete coverage; clipboard unchanged")
        }
        Err(error) if error.contains("coverage is unknown") => {
            println!("real export refused: legacy coverage unknown; clipboard unchanged")
        }
        Err(error) => panic!("real export qualification failed: {error}"),
    }
}

/// Empty history has a truthful zero watermark and needs no raw-page chase.
#[tokio::test]
async fn empty_transcript_copy_is_complete() {
    let (broker, _calls) = FakeBroker::scripted(vec![Ok(tail(0))]);
    let text = export(&*broker, &target()).await.unwrap();
    assert!(text.contains("Snapshot through #0"));
}

/// Canonical pool entry fixture retains the stamped stable member identity and role.
fn entry(seq: u64, body: &str) -> PoolEntryView {
    PoolEntryView {
        seq,
        roster_revision: 1,
        author_kind: AuthorKind::Member,
        author_agent_id: Some(AGENT.parse().unwrap()),
        author_name: Some("Ada".into()),
        author_role: Some("review".into()),
        direction: Direction::Team,
        kind: EntryKind::Message,
        severity: None,
        proposal_seq: None,
        decision: None,
        body: body.into(),
    }
}
/// Public pool page with a separately observed durable watermark.
fn pool_page(entries: Vec<PoolEntryView>, last: Option<u64>, complete: bool) -> Value {
    json!({"pool_id":POOL,"entries":entries,"before_seq":null,"last_seq":last,"complete":complete,
        "status":{"state":"open","roster_revision":1,"goal":"fixture","criteria":[],"current_proposal":null,"members":[],"replaced_members":[]}})
}

/// Complete multi-page pool export uses canonical author context and freezes the initial watermark.
#[tokio::test]
async fn pool_copy_preserves_authors_and_ignores_growing_tail() {
    let (broker, mut calls) = FakeBroker::scripted(vec![
        Ok(pool_page(
            vec![entry(1, "Первый\n  body  \n")],
            Some(2),
            false,
        )),
        Ok(pool_page(
            vec![entry(2, "second"), entry(3, "new tail")],
            Some(3),
            true,
        )),
    ]);
    let text = export(&*broker, &Target::Pool(POOL.parse().unwrap()))
        .await
        .unwrap();
    assert!(text.contains(&format!("Ada (review, {AGENT})")));
    assert!(text.contains("Первый\n  body  \n"));
    assert!(text.contains("second"));
    assert!(!text.contains("new tail"));
    assert_eq!(calls.recv().await.unwrap()["after_seq"], 0);
    assert_eq!(calls.recv().await.unwrap()["after_seq"], 1);
}

/// Copy target freezes at keypress; repeated keys coalesce and cancelled completions cannot overwrite feedback.
#[test]
fn keypress_freezes_target_and_coalesces_requests() {
    let mut app = crate::app::App::new();
    crate::events::apply_action(&mut app, crate::events::Action::Copy);
    assert_eq!(
        app.copy_feedback.as_deref(),
        Some("Select an agent or pool to copy")
    );
    assert!(app.copy_request.is_none());
    app.sessions.push(agent_view(AGENT, AGENT, "running"));
    crate::events::apply_action(&mut app, crate::events::Action::Copy);
    let first = app.copy_request.clone().unwrap();
    app.pools.visible = true;
    app.pools.select(POOL.parse().unwrap());
    crate::events::apply_action(&mut app, crate::events::Action::Copy);
    assert_eq!(app.copy_request, Some(first.clone()));
    crate::events::apply_action(&mut app, crate::events::Action::CancelCopy);
    crate::events::apply_action(&mut app, crate::events::Action::Copy);
    let newer = app.copy_request.clone().unwrap();
    let mut pipeline = crate::events::Pipeline::new(crate::events::FRAME_INTERVAL);
    pipeline.broker_event(
        &mut app,
        crate::events::BrokerEvent::Copied {
            generation: first.generation,
            target: first.target.clone(),
            result: Ok(10),
        },
    );
    assert_eq!(app.copy_request, Some(newer));
    assert!(app.copy_feedback.is_none());
}

/// Cancellation of a genuinely pending broker read cleans up without writing a prefix.
#[tokio::test]
async fn cancellation_stops_pending_copy_without_clipboard_write() {
    /// Pending snapshot reader with a positive admission receipt for the test.
    struct Pending {
        /// Notification that export entered a real asynchronous call.
        entered: tokio::sync::Notify,
    }
    impl Broker for Pending {
        /// Holds every fixture read until its future is cancelled.
        fn call<'a>(&'a self, _: &'a str, _: Value) -> crate::net::BrokerFuture<'a> {
            Box::pin(async move {
                self.entered.notify_one();
                std::future::pending().await
            })
        }
    }
    let broker = Arc::new(Pending {
        entered: tokio::sync::Notify::new(),
    });
    let clipboard = Arc::new(RecordingClipboard::default());
    let (requests, rx) = watch::channel(Some(Request {
        generation: 1,
        target: target(),
    }));
    let (events, _rx) = mpsc::channel(2);
    let job = tokio::spawn(worker(broker.clone(), clipboard.clone(), rx, events));
    tokio::time::timeout(Duration::from_secs(1), broker.entered.notified())
        .await
        .unwrap();
    requests.send(None).unwrap();
    drop(requests);
    tokio::time::timeout(Duration::from_secs(1), job)
        .await
        .unwrap()
        .unwrap();
    assert!(clipboard.written.lock().unwrap().is_empty());
}
