//! Synthetic read-only observation regressions; no engines or external services.
use crate::common;

use agent_run_core::{dispatch, service::Service};
use agent_run_domain::{Error, domain::AgentId, observation::AnswerChunkQuery};
use agent_run_platform::{fs, verify};
use serde_json::{Value, json};
use std::path::Path;

/// Admits and seals an owned synthetic answer, returning exact broker evidence.
fn sealed(home: &common::Home, text: &str) -> (AgentId, verify::Proof) {
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    let root = home.path.join("agents").join(id.as_str());
    fs::private_dir(&root).unwrap();
    let proof = verify::seal(&root, Path::new("answer.md"), text).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET answer_path=?1, answer_bytes=?2, answer_sha256=?3 WHERE id=?4",
            rusqlite::params![
                proof.path.to_str().unwrap(),
                proof.bytes,
                proof.sha256,
                id.as_str()
            ],
        )
        .unwrap();
    (id, proof)
}

/// Builds the strict wire query with the omitted default budget.
fn query(id: &AgentId, proof: &verify::Proof) -> AnswerChunkQuery {
    serde_json::from_value(json!({"agent_id":id,"sequence":1,
        "expected_sha256":proof.sha256,"offset_bytes":0}))
    .unwrap()
}

/// Chunks reconstruct exact multi-byte/control/quoted bytes and their digest,
/// while ordinary full-answer reads keep their original inline threshold.
#[tokio::test]
async fn answer_chunks_reconstruct_large_verified_artifacts_without_writes() {
    let home = common::Home::new();
    let text = "a\"\u{0000}\né🦀".repeat(120_000);
    let (id, proof) = sealed(&home, &text);
    let service = Service::new(home.path.clone());
    let revision = home.store().revision().unwrap();
    let before = home.store().get(&id).unwrap();
    let mut query = query(&id, &proof);
    assert_eq!(query.max_bytes, 65536);
    assert!(service.answer(&id).unwrap()["content"].is_null());
    let mut reconstructed = String::new();
    loop {
        let chunk = service.answer_chunk(&query).unwrap();
        assert_eq!(chunk.sequence, 1);
        assert_eq!(chunk.sha256, proof.sha256);
        assert_eq!(chunk.total_bytes, text.len() as u64);
        assert_eq!(chunk.offset_bytes, query.offset_bytes);
        assert!(chunk.content.len() <= query.max_bytes);
        assert!(text.is_char_boundary(chunk.next_offset as usize));
        assert!(chunk.next_offset > query.offset_bytes);
        reconstructed.push_str(&chunk.content);
        query.offset_bytes = chunk.next_offset;
        if chunk.complete {
            break;
        }
    }
    assert_eq!(reconstructed, text);
    assert_eq!(fs::sha256(reconstructed.as_bytes()), proof.sha256);
    let eof = dispatch::call(
        &service,
        "answer_chunk",
        serde_json::to_value(&query).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(eof["content"], "");
    assert_eq!(eof["complete"], true);
    assert_eq!(eof["next_offset"], proof.bytes);
    assert_eq!(home.store().revision().unwrap(), revision);
    let after = home.store().get(&id).unwrap();
    assert_eq!(before.status, after.status);
    assert_eq!(before.answer_sha256, after.answer_sha256);

    let (inline, _) = sealed(&home, &"x".repeat(verify::INLINE_ANSWER));
    assert_eq!(
        service.answer(&inline).unwrap()["content"]
            .as_str()
            .unwrap()
            .len(),
        verify::INLINE_ANSWER
    );
}

/// Cursor/default validation rejects nulls, unknown fields, invalid receipts,
/// digest syntax, out-of-range budgets, non-boundaries and beyond-EOF offsets.
#[tokio::test]
async fn answer_chunk_boundaries_and_strict_inputs_are_typed() {
    let home = common::Home::new();
    let (id, proof) = sealed(&home, "é🦀x");
    let service = Service::new(home.path.clone());
    let original = serde_json::to_value(query(&id, &proof)).unwrap();
    for (key, value) in [
        ("sequence", json!(0)),
        ("sequence", Value::Null),
        ("sequence", json!(4294967296u64)),
        ("offset_bytes", json!(-1)),
        ("offset_bytes", Value::Null),
        ("offset_bytes", json!(1)),
        ("offset_bytes", json!(8)),
        ("offset_bytes", json!(u64::MAX)),
        ("max_bytes", json!(0)),
        ("max_bytes", json!(131073)),
        ("max_bytes", Value::Null),
        ("max_bytes", json!(1)),
        ("expected_sha256", json!("A".repeat(64))),
        ("path", json!("answer.md")),
    ] {
        let mut input = original.clone();
        input[key] = value;
        assert!(
            matches!(
                dispatch::call(&service, "answer_chunk", input.clone()).await,
                Err(Error::Validation(_))
            ),
            "{input}"
        );
    }
    for key in ["agent_id", "sequence", "expected_sha256", "offset_bytes"] {
        let mut input = original.clone();
        input.as_object_mut().unwrap().remove(key);
        assert!(matches!(
            dispatch::call(&service, "answer_chunk", input).await,
            Err(Error::Validation(_))
        ));
    }
    let mut q = query(&id, &proof);
    q.max_bytes = 5;
    let first = service.answer_chunk(&q).unwrap();
    assert_eq!(first.content, "é");
    assert_eq!(first.next_offset, 2);
    q.offset_bytes = 2;
    q.max_bytes = 4;
    assert_eq!(service.answer_chunk(&q).unwrap().content, "🦀");
    q.max_bytes = 131072;
    assert_eq!(service.answer_chunk(&q).unwrap().content, "🦀x");
}

/// CLI-only pages remain pinned across a resume and reject changed evidence or invalid cursors.
#[tokio::test]
async fn cli_answer_pages_pin_historical_execution_and_proof() {
    let home = common::Home::new();
    let (id, proof) = sealed(&home, "é🦀 original");
    let service = Service::new(home.path.clone());
    let selected = dispatch::call(&service, "_cli_answer", json!({"agent_id":id}))
        .await
        .unwrap();
    assert_eq!(selected["sequence"], 1);
    assert_eq!(selected["inline"], true);
    assert!(selected["answer"]["content"].is_null());

    // A continuation after selection must not redirect the pinned page.
    let (child, _) = sealed(&home, "newer answer");
    home.store()
        .conn
        .execute(
            "UPDATE agents SET root_agent_id=?1,parent_agent_id=?1,sequence=2 WHERE id=?2",
            rusqlite::params![id.as_str(), child.as_str()],
        )
        .unwrap();
    let mut page = json!({"query":{"agent_id":id,"sequence":1,"expected_sha256":&proof.sha256,
        "offset_bytes":0,"max_bytes":6},"proof_token":selected["proof_token"]});
    let first = dispatch::call(&service, "_cli_answer_chunk", page.clone())
        .await
        .unwrap();
    assert_eq!(first["content"], "é🦀");
    assert_eq!(first["next_offset"], 6);
    page["query"]["offset_bytes"] = json!(6);
    let second = dispatch::call(&service, "_cli_answer_chunk", page.clone())
        .await
        .unwrap();
    assert_eq!(second["content"], " origi");
    page["query"]["offset_bytes"] = json!(1);
    assert!(matches!(
        dispatch::call(&service, "_cli_answer_chunk", page.clone()).await,
        Err(Error::Validation(_))
    ));
    page["query"]["offset_bytes"] = json!(0);
    page["proof_token"] = json!("0".repeat(64));
    assert!(matches!(
        dispatch::call(&service, "_cli_answer_chunk", page.clone()).await,
        Err(Error::AnswerIntegrity(_))
    ));

    // Even the same hash and size on another path is a different proof.
    page["proof_token"] = selected["proof_token"].clone();
    let root = home.path.join("agents").join(id.as_str());
    let moved = verify::seal(&root, Path::new("alternate.md"), "é🦀 original").unwrap();
    assert_ne!(moved.path, proof.path);
    assert_eq!(moved.bytes, proof.bytes);
    assert_eq!(moved.sha256, proof.sha256);
    home.store()
        .conn
        .execute(
            "UPDATE agents SET answer_path=?1 WHERE id=?2",
            rusqlite::params![moved.path.to_str().unwrap(), id.as_str()],
        )
        .unwrap();
    assert!(matches!(
        dispatch::call(&service, "_cli_answer_chunk", page.clone()).await,
        Err(Error::AnswerIntegrity(_))
    ));
    page["query"]["expected_sha256"] = json!("0".repeat(64));
    assert!(matches!(
        dispatch::call(&service, "_cli_answer_chunk", page).await,
        Err(Error::AnswerHashMismatch)
    ));
    assert!(!agent_run_domain::tools::is_tool("_cli_answer"));
    assert!(!agent_run_domain::tools::is_tool("_cli_answer_chunk"));
}

/// A synthetic continuation with even the same digest invalidates old cursors;
/// changed hashes and missing current answers have separate stable error codes.
#[test]
fn answer_chunks_fence_sequence_and_hash_across_continuations() {
    let home = common::Home::new();
    let (id, proof) = sealed(&home, "same answer");
    let service = Service::new(home.path.clone());
    let mut q = query(&id, &proof);
    q.expected_sha256 = "0".repeat(64).parse().unwrap();
    assert!(matches!(
        service.answer_chunk(&q),
        Err(Error::AnswerHashMismatch)
    ));
    q.expected_sha256 = proof.sha256.parse().unwrap();
    let (next, _) = sealed(&home, "same answer");
    home.store()
        .conn
        .execute(
            "UPDATE agents SET root_agent_id=?1, parent_agent_id=?1, sequence=2 WHERE id=?2",
            rusqlite::params![id.as_str(), next.as_str()],
        )
        .unwrap();
    assert!(matches!(
        service.answer_chunk(&q),
        Err(Error::ExecutionChanged)
    ));
    q.sequence = std::num::NonZeroU32::new(2).unwrap();
    assert_eq!(service.answer_chunk(&q).unwrap().sequence, 2);
    home.store()
        .conn
        .execute(
            "UPDATE agents SET answer_path=NULL WHERE id=?1",
            [next.as_str()],
        )
        .unwrap();
    assert!(matches!(
        service.answer_chunk(&q),
        Err(Error::AnswerUnavailable)
    ));
}

/// Chunk reads reuse full payload/seal/UTF-8/no-follow checks, including when
/// the damaged bytes are outside the requested prefix. No partial text escapes.
#[test]
fn answer_chunks_reject_corrupt_missing_seals_and_symlinks() {
    for corruption in [
        "payload",
        "sidecar",
        "marker",
        "missing",
        "symlink",
        "sidecar_symlink",
        "utf8",
    ] {
        let home = common::Home::new();
        let (id, mut proof) = sealed(&home, "verified answer");
        let service = Service::new(home.path.clone());
        let root = proof.path.parent().unwrap();
        match corruption {
            "payload" => std::fs::write(&proof.path, "verified answeX").unwrap(),
            "sidecar" => std::fs::write(root.join("answer.md.proof.json"), "{}").unwrap(),
            "marker" => std::fs::write(root.join(".answer-format"), "wrong").unwrap(),
            "missing" => std::fs::remove_file(root.join("answer.md.proof.json")).unwrap(),
            "symlink" => {
                let outside = home.path.join("outside");
                std::fs::write(&outside, "verified answer").unwrap();
                std::fs::remove_file(&proof.path).unwrap();
                std::os::unix::fs::symlink(outside, &proof.path).unwrap();
            }
            "sidecar_symlink" => {
                let path = root.join("answer.md.proof.json");
                let outside = home.path.join("outside-proof");
                std::fs::rename(&path, &outside).unwrap();
                std::os::unix::fs::symlink(outside, path).unwrap();
            }
            "utf8" => {
                let bytes = [0xff; 15];
                std::fs::write(&proof.path, bytes).unwrap();
                proof.sha256 = fs::sha256(&bytes);
                home.store()
                    .conn
                    .execute(
                        "UPDATE agents SET answer_sha256=?1 WHERE id=?2",
                        rusqlite::params![proof.sha256, id.as_str()],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let mut q = query(&id, &proof);
        q.max_bytes = 1;
        let error = service.answer_chunk(&q).unwrap_err();
        if corruption == "symlink" {
            assert!(matches!(error, Error::PathEscape(_)), "{error:?}");
        } else {
            assert!(
                matches!(error, Error::AnswerIntegrity(_)),
                "{corruption}: {error:?}"
            );
        }
    }
}

/// Discovery is compiled, strict and provider/store free, and exposes exactly
/// the registry's read methods plus their schema declarations.
#[tokio::test]
async fn capabilities_and_ping_are_read_only_without_a_home() {
    let service = Service::new("/nonexistent-observation-fixture".into());
    assert_eq!(
        dispatch::call(&service, "ping", json!({})).await.unwrap(),
        json!({"ok":true})
    );
    let caps = dispatch::call(&service, "capabilities", json!({}))
        .await
        .unwrap();
    let typed: agent_run_domain::observation::CapabilitiesView =
        serde_json::from_value(caps.clone()).unwrap();
    assert_eq!(typed.schema_version, 1);
    assert_eq!(typed.product, "agent-run");
    assert_eq!(typed.version, env!("CARGO_PKG_VERSION"));
    assert!(typed.source_revision.is_none());
    let expected: Vec<_> = agent_run_domain::registry()
        .iter()
        .filter(|t| t.annotations.read_only_hint)
        .map(|t| t.name.clone())
        .collect();
    assert_eq!(
        typed
            .read_methods
            .iter()
            .map(|m| m.name.clone())
            .collect::<Vec<_>>(),
        expected
    );
    for name in ["answer_chunk", "capabilities"] {
        let method = typed.read_methods.iter().find(|m| m.name == name).unwrap();
        assert_eq!(
            method.input_schema,
            agent_run_domain::tool(name).unwrap().input_schema
        );
        assert!(method.result_shape.is_some());
        assert!(
            !agent_run_domain::tools::worker_registry()
                .iter()
                .any(|t| t.name == name)
        );
    }
    assert!(matches!(
        dispatch::call(&service, "capabilities", json!({"extra":true})).await,
        Err(Error::Validation(_))
    ));
}

/// A resumed lineage keeps the viewer's selected execution and scopes explicit reads.
#[tokio::test]
async fn cli_transcript_selection_stays_pinned_during_continuation() {
    let home = common::Home::new();
    let mut store = home.store();
    let request = home.request();
    let (root, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    store
        .message(&root, "assistant", "old", None, None)
        .unwrap();
    drop(store);
    let service = Service::new(home.path.clone());
    let selected = dispatch::call(&service, "_cli_transcript_select", json!({"agent_id":root}))
        .await
        .unwrap();
    let mut store = home.store();
    let (child, _) = store
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET root_agent_id=?1,parent_agent_id=?1,sequence=2 WHERE id=?2",
            rusqlite::params![root.as_str(), child.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='succeeded' WHERE id=?1",
            [root.as_str()],
        )
        .unwrap();
    store
        .message(&child, "assistant", "new", None, None)
        .unwrap();
    drop(store);
    let status = dispatch::call(&service, "_cli_transcript_status", selected.clone())
        .await
        .unwrap();
    assert_eq!(status["selection"], selected);
    assert_eq!(status["status"], "succeeded");
    for (lineage, expected) in [(true, vec!["old", "new"]), (false, vec!["old"])] {
        let response = dispatch::call(
            &service,
            "_cli_transcript_page",
            json!({
                "selection":selected,"query":{"cursor":0,"limit":10,"view":"raw"},
                "lineage":lineage,"recovery":false,"offset_bytes":0,"expected_sha256":null
            }),
        )
        .await
        .unwrap();
        assert_eq!(response["complete"], true);
        let page: Value = serde_json::from_str(response["content"].as_str().unwrap()).unwrap();
        assert_eq!(
            page["messages"]
                .as_array()
                .unwrap()
                .iter()
                .map(|message| message["content"].as_str().unwrap())
                .collect::<Vec<_>>(),
            expected
        );
    }
    let mut invalid = selected;
    invalid["sequence"] = json!(2);
    assert!(matches!(
        dispatch::call(&service, "_cli_transcript_status", invalid).await,
        Err(Error::ExecutionChanged)
    ));
}

/// Legacy raw content larger than the page budget is rejected before page materialization.
#[tokio::test]
async fn cli_transcript_rejects_oversized_legacy_row() {
    let home = common::Home::new();
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .message(&id, "assistant", "fixture", None, None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE messages SET content=CAST(zeroblob(?1) AS TEXT) WHERE agent_id=?2",
            rusqlite::params![
                (dispatch::CLI_TRANSCRIPT_MAX_PAGE_BYTES + 1) as i64,
                id.as_str()
            ],
        )
        .unwrap();
    drop(store);
    let service = Service::new(home.path.clone());
    let selection = dispatch::call(&service, "_cli_transcript_select", json!({"agent_id":id}))
        .await
        .unwrap();
    let result = dispatch::call(
        &service,
        "_cli_transcript_page",
        json!({
            "selection":selection,"query":{"cursor":0,"limit":10,"view":"raw"},
            "lineage":true,"recovery":false,"offset_bytes":0,"expected_sha256":null
        }),
    )
    .await;
    assert!(
        matches!(result, Err(Error::Runtime(message)) if message == "transcript page exceeds 64 MiB limit")
    );
}
