//! Journal batching of the supervisor's persistence facade against a real
//! writer: batches are cut by exact encoded size, never lose a row and never
//! exceed the operation's bound.
mod common;
use serde_json::json;

/// A transcript whose chunks are all control characters encodes six times
/// larger than it is long: 32 valid 16 KiB chunks exceed the 2 MiB decoded
/// bound of one `AppendJournal`, so they must be cut into several batches and
/// every chunk must reach the journal in order.
#[tokio::test]
async fn control_character_chunks_are_batched_by_exact_encoded_size() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    drop(store);
    let harness = common::writer::Harness::start(&home.path);
    let mut persist = harness.supervisor(&home.path, prep, &id).await;
    // 32 chunks of 16 KiB, each a different control character so order is visible.
    let mut expected = Vec::new();
    for chunk in 0..32_u8 {
        let text: String = std::iter::repeat_n(char::from(chunk % 8 + 1), 16 * 1024).collect();
        persist
            .journal(&id, "assistant", &text, None, None)
            .await
            .expect("a valid chunk is never lost");
        expected.push(text);
    }
    persist.flush().await.expect("the remainder is written");
    let rows: Vec<String> = persist
        .conn
        .prepare("SELECT content FROM messages WHERE agent_id=? AND role='assistant' ORDER BY seq")
        .unwrap()
        .query_map([id.as_str()], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows, expected, "every chunk, in observation order");
}

/// A row that can never fit one batch is refused when it is offered, and it
/// does not poison the buffer: later rows still reach the journal.
#[tokio::test]
async fn an_oversize_row_is_refused_and_does_not_block_later_rows() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    drop(store);
    let harness = common::writer::Harness::start(&home.path);
    let mut persist = harness.supervisor(&home.path, prep, &id).await;
    let huge = json!({"blob": "x".repeat(2 * 1024 * 1024)});
    assert!(persist.event(&id, "too_big", &huge).await.is_err());
    assert!(!persist.has_pending(), "the refused row was not buffered");
    persist
        .event_durable(&id, "after_refusal", &json!({"n": 1}))
        .await
        .expect("later rows are unaffected");
    let count: i64 = persist
        .conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent_id=? AND kind='after_refusal'",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

/// Escaping inflates different text differently (controls six-fold, BMP
/// characters two-fold, astral characters three-fold in the ASCII wire body,
/// quotes and backslashes two-fold): a long mixed transcript is written in
/// order, none of it lost, whatever the mix.
#[tokio::test]
async fn mixed_unicode_and_escaped_chunks_are_never_lost() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    drop(store);
    let harness = common::writer::Harness::start(&home.path);
    let mut persist = harness.supervisor(&home.path, prep, &id).await;
    let unit = ["\u{1}", "\u{1d11e}", "\u{4e2d}", "\"\\", "a"];
    let mut expected = Vec::new();
    for chunk in 0..60 {
        let text = unit[chunk % unit.len()].repeat(3 * 1024);
        persist
            .journal(&id, "assistant", &text, None, None)
            .await
            .expect("a valid chunk is never lost");
        expected.push(text);
    }
    persist.flush().await.unwrap();
    let rows: Vec<String> = persist
        .conn
        .prepare("SELECT content FROM messages WHERE agent_id=? AND role='assistant' ORDER BY seq")
        .unwrap()
        .query_map([id.as_str()], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows, expected);
}
