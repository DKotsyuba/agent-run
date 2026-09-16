//! Service facade regressions that use durable state only and never launch an engine.

mod common;

use agent_run_core::service::{Query, Service};
use agent_run_domain::{domain::Outcome, Error};
use agent_run_platform::{fs, verify};
use std::path::Path;

/// Mirrors Python `tests/test_service.py::AgentServiceTests::test_answer_verifies_path_size_hash_and_bounds_inline_content`.
///
/// The public answer envelope exposes only evidence that is still verified
/// against the stored path, byte count, and digest at read time.
#[test]
fn answer_rechecks_persisted_proof_before_exposing_content() {
    let home = common::Home::new();
    let (id, _) = home
        .store()
        .admit(&home.request(), &home.config, &serde_json::json!({}), None)
        .unwrap();
    let root = home.path.join("agents").join(id.as_str());
    std::fs::create_dir_all(&root).unwrap();
    fs::private_dir(&root).unwrap();
    let proof = verify::seal(&root, Path::new("answer.md"), "trusted text").unwrap();
    let mut store = home.store();
    store.running(&id, 42).unwrap();
    store
        .finish(&id, &Outcome::success(None), Some(&proof), None)
        .unwrap();

    let service = Service::new(home.path.clone());
    let answer = service.answer(&id).unwrap();
    assert_eq!(answer["available"], true);
    assert_eq!(answer["content"], "trusted text");
    assert_eq!(answer["size_bytes"], proof.bytes);
    assert_eq!(answer["sha256"], proof.sha256);
    assert_eq!(answer["proof_version"], 2);

    std::fs::write(&proof.path, "tampered text").unwrap();
    assert!(matches!(
        service.answer(&id),
        Err(Error::AnswerIntegrity(_))
    ));
}

/// Mirrors Python `tests/test_service.py::AgentServiceTests::test_list_has_exact_total_and_explicit_offset_completeness`.
/// Mirrors Python `tests/test_service.py::AgentServiceTests::test_list_and_transcript_share_the_bounded_page_limit`.
///
/// Pagination returns an exact durable total and makes both the next offset
/// and completeness explicit; invalid page bounds fail before opening state.
#[tokio::test]
async fn list_has_bounded_explicit_pagination() {
    let home = common::Home::new();
    home.store()
        .admit(&home.request(), &home.config, &serde_json::json!({}), None)
        .unwrap();
    home.store()
        .admit(
            &agent_run_domain::domain::StartRequest {
                request_id: Some("second".into()),
                ..home.request()
            },
            &home.config,
            &serde_json::json!({}),
            None,
        )
        .unwrap();
    let service = Service::new(home.path.clone());
    let first = service
        .list(Query {
            limit: 1,
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(first["total"], 2);
    assert_eq!(first["items"].as_array().unwrap().len(), 1);
    assert_eq!(first["next_offset"], 1);
    assert_eq!(first["complete"], false);

    let last = service
        .list(Query {
            offset: 1,
            limit: 1,
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(last["next_offset"], serde_json::Value::Null);
    assert_eq!(last["complete"], true);
    assert!(Query {
        limit: 0,
        ..Query::default()
    }
    .validate()
    .is_err());
}
