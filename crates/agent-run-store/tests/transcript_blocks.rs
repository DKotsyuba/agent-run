//! Bounded block views and native tool-evidence contracts over real rows.

mod common;

use agent_run_domain::{
    domain::AgentId,
    transcript::{TranscriptQuery, TranscriptView},
};
use agent_run_store::Store;
use serde_json::json;

/// Admits one fresh agent through the shared fixture home.
fn agent(store: &mut Store, home: &common::Home) -> AgentId {
    store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap()
        .0
}

/// Links `child` into `parent`'s lineage as a later execution.
fn lineage_child(store: &Store, parent: &AgentId, child: &AgentId) {
    store
        .conn
        .execute(
            "UPDATE agents SET parent_agent_id=?,root_agent_id=?,sequence=2 WHERE id=?",
            rusqlite::params![parent.as_str(), parent.as_str(), child.as_str()],
        )
        .unwrap();
}

/// One forward block request.
fn forward(cursor: i64, limit: usize) -> TranscriptQuery {
    TranscriptQuery {
        cursor,
        limit,
        view: TranscriptView::Blocks,
        tail_blocks: None,
        before_cursor: None,
    }
}

/// One bounded tail request for the last `blocks` blocks.
fn tail(blocks: usize) -> TranscriptQuery {
    TranscriptQuery {
        view: TranscriptView::Blocks,
        tail_blocks: Some(blocks),
        ..TranscriptQuery::default()
    }
}

/// Only consecutive fragments sharing a known native identity, role, name and
/// execution join one block; NULL identities, interleaved tool or user rows
/// and unrelated calls never merge.
#[test]
fn blocks_join_only_consecutive_known_identities() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    for (role, name, raw_ref, content) in [
        ("assistant", None, Some("msg_1"), "Hello "),
        ("assistant", None, Some("msg_1"), "world"),
        ("assistant", None, Some("msg_2"), "other message"),
        ("assistant", None, None, "no identity"),
        ("assistant", None, None, "still no identity"),
        ("user", None, Some("msg_1"), "same ref other role"),
        ("assistant", Some("speaker"), Some("msg_3"), "named"),
        ("assistant", None, Some("msg_3"), "unnamed same ref"),
    ] {
        store.message(&id, role, content, name, raw_ref).unwrap();
    }
    let page = store
        .transcript_query(&id, &forward(0, 200), false)
        .unwrap();
    let messages = page["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 7, "{messages:?}");
    assert_eq!(messages[0]["content"], "Hello world");
    assert_eq!(messages[0]["first_seq"], 1);
    assert_eq!(messages[0]["last_seq"], 2);
    assert_eq!(messages[0]["starts_block"], true);
    assert_eq!(messages[2]["content"], "no identity");
    assert_eq!(messages[2]["starts_block"], true);
    assert_eq!(
        messages[3]["starts_block"], true,
        "NULL identity never merges"
    );
    assert_eq!(messages[4]["role"], "user", "{messages:?}");
    assert_eq!(messages[5]["content"], "named");
    assert_eq!(messages[6]["content"], "unnamed same ref");
    assert_eq!(messages[6]["starts_block"], true, "name change splits");
}

/// Identical raw refs of different executions stay separate blocks even in a
/// lineage read; the boundary is reported without exposing execution ids.
#[test]
fn equal_refs_of_different_executions_never_merge() {
    let home = common::Home::new();
    let mut store = home.store();
    let root = agent(&mut store, &home);
    let child = agent(&mut store, &home);
    lineage_child(&store, &root, &child);
    store
        .message(&root, "assistant", "first ", None, Some("msg_x"))
        .unwrap();
    store
        .message(&root, "assistant", "execution", None, Some("msg_x"))
        .unwrap();
    store
        .message(&child, "assistant", "second ", None, Some("msg_x"))
        .unwrap();
    store
        .message(&child, "assistant", "execution", None, Some("msg_x"))
        .unwrap();
    let page = store
        .transcript_query(&root, &forward(0, 200), true)
        .unwrap();
    let messages = page["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2, "{messages:?}");
    assert_eq!(messages[0]["content"], "first execution");
    assert_eq!(messages[1]["content"], "second execution");
    assert_eq!(
        messages[1]["starts_block"], true,
        "a fresh execution opens a boundary block"
    );
    assert_eq!(messages[0]["starts_block"], true);
    let text = page.to_string();
    assert!(!text.contains("attempt_id"), "{text}");
    assert!(!text.contains("run_id"), "{text}");
}

/// Tail requests return the last N blocks chronologically with an exclusive
/// previous cursor; before_cursor pages further back without gaps.
#[test]
fn tail_blocks_page_backwards_with_exclusive_cursors() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    for block in 0..5 {
        store
            .message(
                &id,
                "assistant",
                &format!("m{block}a "),
                None,
                Some(&format!("m{block}")),
            )
            .unwrap();
        store
            .message(
                &id,
                "assistant",
                &format!("m{block}b"),
                None,
                Some(&format!("m{block}")),
            )
            .unwrap();
    }
    let page = store.transcript_query(&id, &tail(2), false).unwrap();
    let messages = page["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2, "{messages:?}");
    assert_eq!(messages[0]["content"], "m3a m3b");
    assert_eq!(messages[1]["content"], "m4a m4b");
    assert_eq!(page["direction"], "backward");
    assert_eq!(page["complete"], false);
    let previous = page["previous_cursor"].as_i64().unwrap();
    assert_eq!(previous, messages[0]["first_seq"]);
    let older = TranscriptQuery {
        before_cursor: Some(previous),
        ..tail(2)
    };
    let page = store.transcript_query(&id, &older, false).unwrap();
    let messages = page["messages"].as_array().unwrap();
    assert_eq!(messages[0]["content"], "m1a m1b");
    assert_eq!(messages[1]["content"], "m2a m2b");
    assert_eq!(
        page["previous_cursor"].as_i64().unwrap(),
        messages[0]["first_seq"]
    );
    // The final backward page reaches the beginning and says so.
    let oldest = TranscriptQuery {
        before_cursor: Some(page["previous_cursor"].as_i64().unwrap()),
        ..tail(2)
    };
    let page = store.transcript_query(&id, &oldest, false).unwrap();
    let messages = page["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["content"], "m0a m0b");
    assert_eq!(page["complete"], true);
    assert!(page["previous_cursor"].is_null());
}

/// Partial blocks outside a page are flagged truthfully in both directions.
#[test]
fn partial_blocks_are_flagged_at_page_boundaries() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    store.message(&id, "user", "question", None, None).unwrap();
    for part in ["a", "b", "c"] {
        store
            .message(&id, "assistant", part, None, Some("stream"))
            .unwrap();
    }
    // Blocks are the paging unit: limit one returns the whole block that
    // starts inside the page, flagging the fragment left before it.
    let page = store.transcript_query(&id, &forward(2, 1), false).unwrap();
    let messages = page["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["content"], "bc");
    assert_eq!(messages[0]["partial_before"], true);
    assert_eq!(messages[0]["starts_block"], false);
    assert_eq!(page["complete"], true);
    // The reverse tail of one block returns that whole block.
    let tail_page = store.transcript_query(&id, &tail(1), false).unwrap();
    let messages = tail_page["messages"].as_array().unwrap();
    assert_eq!(messages[0]["content"], "abc");
    assert_eq!(messages[0]["partial_after"], false);
    assert_eq!(messages[0]["partial_before"], false);
}

/// Oversized inline rows are spooled by the writer, so a block reports
/// incomplete content with an opaque reference instead of dropping text; the
/// page-level byte bound stops before exceeding its budget and says so.
#[test]
fn oversized_blocks_report_omitted_content() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    let long = "é".repeat(40_000);
    store
        .message(&id, "assistant", &long, None, Some("big"))
        .unwrap();
    store
        .message(&id, "assistant", "tail", None, Some("small"))
        .unwrap();
    let page = store
        .transcript_query(&id, &forward(0, 200), false)
        .unwrap();
    let messages = page["messages"].as_array().unwrap();
    let big = &messages[0];
    assert_eq!(big["content_complete"], false);
    assert!(
        big["content"].as_str().unwrap().contains("spooled"),
        "the stub explains the omission: {}",
        big["content"]
    );
    assert!(big["raw_ref"].as_str().unwrap().starts_with("message."));
    let tail_page = store.transcript_query(&id, &tail(1), false).unwrap();
    let messages = tail_page["messages"].as_array().unwrap();
    assert_eq!(messages[0]["content"], "tail");

    // The page byte budget bounds many inline blocks explicitly.
    let chunk = "x".repeat(30_000);
    for block in 0..12 {
        store
            .message(&id, "assistant", &chunk, None, Some(&format!("p{block}")))
            .unwrap();
    }
    let page = store
        .transcript_query(&id, &forward(0, 200), false)
        .unwrap();
    assert_eq!(page["complete"], false, "the byte budget stopped the page");
    let bytes: usize = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["content"].as_str().unwrap().len())
        .sum();
    assert!(bytes <= 256 * 1024, "{bytes}");
}

/// Raw remains the default representation and keeps its historical shape,
/// with the additive evidence and boundary flags only.
#[test]
fn raw_view_stays_the_compatible_default() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    store
        .message(&id, "assistant", "one", None, Some("m1"))
        .unwrap();
    store
        .message(&id, "assistant", "two", None, Some("m1"))
        .unwrap();
    let legacy = store.transcript(&id, 0, 10).unwrap();
    let query = TranscriptQuery::default();
    let routed = store.transcript_query(&id, &query, false).unwrap();
    assert_eq!(legacy["messages"], routed["messages"]);
    assert!(routed["view"].is_null());
    assert_eq!(
        routed["messages"][0]["starts_block"], true,
        "raw rows carry the boundary flag"
    );
}

/// Invalid bounds and conflicting reverse selections reject before any read.
#[test]
fn transcript_query_rejects_invalid_bounds() {
    let home = common::Home::new();
    let store = home.store();
    let id: AgentId = "ag-20260101-000000-0000000001".parse().unwrap();
    for query in [
        TranscriptQuery {
            cursor: -1,
            ..TranscriptQuery::default()
        },
        TranscriptQuery {
            limit: 0,
            ..TranscriptQuery::default()
        },
        TranscriptQuery {
            limit: 1001,
            ..TranscriptQuery::default()
        },
        tail(0),
        tail(201),
        TranscriptQuery {
            before_cursor: Some(0),
            ..tail(1)
        },
        TranscriptQuery {
            cursor: 5,
            ..tail(1)
        },
        TranscriptQuery {
            view: TranscriptView::Raw,
            tail_blocks: Some(1),
            ..TranscriptQuery::default()
        },
    ] {
        assert!(query.validate().is_err(), "{query:?}");
        assert!(store.transcript_query(&id, &query, false).is_err());
    }
}

/// Native error evidence is only accepted on tool results with an allowlisted
/// provenance field; anything else stays unknown rather than guessed.
#[test]
fn message_error_evidence_is_validated() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    assert!(store
        .message_with_error(
            &id,
            "assistant",
            "x",
            None,
            None,
            Some((true, "claude.is_error"))
        )
        .is_err());
    assert!(store
        .message_with_error(
            &id,
            "tool_result",
            "x",
            None,
            Some("t1"),
            Some((false, "agent.exit_code"))
        )
        .is_err());
    store
        .message_with_error(
            &id,
            "tool_result",
            "ok",
            Some("Bash"),
            Some("t1"),
            Some((false, "claude.is_error")),
        )
        .unwrap();
    store
        .message_with_error(
            &id,
            "tool_result",
            "boom",
            Some("Bash"),
            Some("t2"),
            Some((true, "codex.command.exitCode")),
        )
        .unwrap();
    let page = store.transcript_query(&id, &forward(0, 10), false).unwrap();
    let messages = page["messages"].as_array().unwrap();
    assert_eq!(messages[0]["error"], false);
    assert_eq!(messages[0]["error_source"], "claude.is_error");
    assert_eq!(messages[1]["error"], true);
    assert_eq!(messages[1]["error_source"], "codex.command.exitCode");
}

/// Tool counts stay wholly unknown without versioned native observers, count
/// one invocation per unique native id across started/completed/fragments,
/// and only explicit native flags mark failures — never error-shaped words.
#[test]
fn tool_counts_are_native_evidence_only() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    store
        .message_with_error(&id, "tool_call", "", Some("Bash"), Some("t1"), None)
        .unwrap();
    store
        .message_with_error(
            &id,
            "tool_result",
            "error: boom",
            Some("Bash"),
            Some("t1"),
            Some((false, "claude.is_error")),
        )
        .unwrap();
    let unknown = store.tool_counts(&id).unwrap();
    assert_eq!(unknown, Default::default());
    // The observer marks coverage for this execution's attempt.
    store
        .event(
            &id,
            "native_tool_observer_v1",
            &json!({"protocol":"claude","version":1}),
        )
        .unwrap();
    let counted = store.tool_counts(&id).unwrap();
    assert_eq!(counted.calls, Some(1));
    assert_eq!(
        counted.failed,
        Some(0),
        "words never override native success"
    );
    assert_eq!(counted.unknown_results, Some(0));
    // A duplicate completion of the same native id adds no invocation.
    store
        .message_with_error(
            &id,
            "tool_result",
            "error: boom",
            Some("Bash"),
            Some("t1"),
            Some((false, "claude.is_error")),
        )
        .unwrap();
    assert_eq!(store.tool_counts(&id).unwrap().calls, Some(1));
    // An explicitly failed invocation counts, and a gap event restores
    // unknown coverage for the whole execution.
    store
        .message_with_error(&id, "tool_call", "", Some("Bash"), Some("t2"), None)
        .unwrap();
    store
        .message_with_error(
            &id,
            "tool_result",
            "exit 1",
            Some("Bash"),
            Some("t2"),
            Some((true, "codex.command.exitCode")),
        )
        .unwrap();
    let counted = store.tool_counts(&id).unwrap();
    assert_eq!(counted.calls, Some(2));
    assert_eq!(counted.failed, Some(1));
    store
        .event(&id, "native_tool_coverage_gap_v1", &json!({}))
        .unwrap();
    assert_eq!(store.tool_counts(&id).unwrap(), Default::default());
}

/// A result without reported evidence and a result-less call both count as
/// unknown results; failed stays null while any result is unknown.
#[test]
fn tool_counts_keep_failed_null_while_results_are_unknown() {
    let home = common::Home::new();
    let mut store = home.store();
    let id = agent(&mut store, &home);
    store
        .event(
            &id,
            "native_tool_observer_v1",
            &json!({"protocol":"codex","version":1}),
        )
        .unwrap();
    store
        .message_with_error(&id, "tool_call", "", Some("ls"), Some("c1"), None)
        .unwrap();
    store
        .message_with_error(&id, "tool_result", "listed", Some("ls"), Some("c1"), None)
        .unwrap();
    store
        .message_with_error(&id, "tool_call", "", Some("ls"), Some("c2"), None)
        .unwrap();
    let counted = store.tool_counts(&id).unwrap();
    assert_eq!(counted.calls, Some(2));
    assert_eq!(counted.unknown_results, Some(2));
    assert_eq!(counted.failed, None);
}
