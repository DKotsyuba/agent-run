//! Blocking wait behavior against durable state and observer deadlines.

mod common;

use agent_run_core::{
    domain::{AgentId, Status},
    service::Service,
};
use agent_run_store::Store;
use serde_json::Value;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

/// Admits one fixture agent and returns its durable identifier.
fn admitted(home: &common::Home) -> AgentId {
    let mut store = home.store();
    store
        .admit(&home.request(), &home.config, &serde_json::json!({}), None)
        .expect("fixture admission")
        .0
}

/// Forces a fixture row to one terminal or active state for observer tests.
fn set_status(home: &common::Home, id: &AgentId, status: Status) {
    let store = home.store();
    store
        .conn
        .execute(
            "UPDATE agents SET status=? WHERE id=?",
            rusqlite::params![status.as_str(), id.as_str()],
        )
        .expect("fixture status update");
}

/// Returns the JSON status string from a wait result.
fn status(value: &Value) -> &str {
    value["status"].as_str().expect("wait status")
}

/// Mirrors `tests/test_wait.py::WaitAgentTests::test_failure_cancellation_and_timeout_map_to_2_3_and_4`
#[tokio::test]
async fn test_failure_cancellation_and_timeout_map_to_2_3_and_4() {
    for (terminal, expected) in [
        (Status::Failed, "failed"),
        (Status::Cancelled, "cancelled"),
        (Status::TimedOut, "timed_out"),
        (Status::Lost, "lost"),
    ] {
        let home = common::Home::new();
        let id = admitted(&home);
        set_status(&home, &id, terminal);
        let result = Service::new(home.path.clone())
            .wait(&id, Some(0.0))
            .await
            .expect("terminal wait");
        assert_eq!(status(&result), expected);
    }
}

/// Mirrors `tests/test_wait.py::WaitAgentTests::test_already_terminal_agent_returns_without_sleeping`
#[tokio::test]
async fn test_already_terminal_agent_returns_without_sleeping() {
    let home = common::Home::new();
    let id = admitted(&home);
    set_status(&home, &id, Status::Succeeded);
    let started = Instant::now();

    let result = Service::new(home.path.clone())
        .wait(&id, Some(0.0))
        .await
        .expect("terminal wait");

    assert_eq!(status(&result), "succeeded");
    assert!(started.elapsed().as_secs_f64() < 1.0);
}

/// Mirrors `tests/test_wait.py::WaitAgentTests::test_a_short_poll_request_is_clamped_to_one_second`
#[tokio::test]
async fn test_a_short_poll_request_is_clamped_to_one_second() {
    let home = common::Home::new();
    let id = admitted(&home);
    set_status(&home, &id, Status::Running);
    let result = Service::new(home.path.clone())
        .wait(&id, Some(0.0))
        .await
        .expect("bounded wait");

    assert_eq!(status(&result), "running");
    assert_eq!(result["terminal"], false);
}

/// Mirrors `tests/test_wait.py::WaitAgentTests::test_no_timeout_keeps_polling_until_the_run_ends`
#[tokio::test]
async fn test_no_timeout_keeps_polling_until_the_run_ends() {
    let home = common::Home::new();
    let id = admitted(&home);
    set_status(&home, &id, Status::Running);
    let service = Service::new(home.path.clone());
    let changed = Arc::new(AtomicBool::new(false));
    let signal = Arc::clone(&changed);
    let update_home = home.path.clone();
    let update_id = id.clone();
    let updater = tokio::spawn(async move {
        while !signal.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        let store = Store::open(&update_home).expect("update store");
        store
            .conn
            .execute(
                "UPDATE agents SET status='succeeded' WHERE id=?",
                [update_id.as_str()],
            )
            .expect("terminal transition");
    });
    changed.store(true, Ordering::Release);

    let result = service.wait(&id, None).await.expect("unbounded wait");
    updater.await.expect("updater joins");
    assert_eq!(status(&result), "succeeded");
}
