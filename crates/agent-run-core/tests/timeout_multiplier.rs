//! `core.timeout_multiplier` at the schema-1 admission boundary.
//!
//! The multiplier is applied once when an admission materializes its effective
//! timeout, so the authoritative `agents.timeout_seconds` column — the value
//! every deadline consumer reads — carries the scaled allowance, and a request
//! whose scaled timeout leaves the shared run-timeout bound is refused before
//! any row exists. Provider-side scaling (the schema-2 surface) is covered in
//! `agent-run`'s `provider_initial` tests.

mod common;

use agent_run_core::service::Service;
use agent_run_domain::Error;
use serde_json::json;

/// The store's default fallback persists the effective default allowance
/// (480 seconds times the default 1.2 multiplier), not the raw base.
#[test]
fn store_default_fallback_persists_the_effective_allowance() {
    let home = common::Home::new();
    let (id, created) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    assert!(created);
    let stored: f64 = home
        .store()
        .conn
        .query_row(
            "SELECT timeout_seconds FROM agents WHERE id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored, 576.0);
    assert_eq!(home.store().get(&id).unwrap().request.timeout_seconds, None);
}

/// A start whose requested timeout is valid on its own but overflows the
/// ceiling once multiplied is refused before any row is written.
#[tokio::test]
async fn start_refuses_a_timeout_that_overflows_once_multiplied() {
    let home = common::Home::new();
    let mut request = home.request();
    request.timeout_seconds = Some(2_500_000.0);
    let error = Service::new(home.path.clone())
        .start(request)
        .await
        .expect_err("2500000 times 1.2 exceeds 2592000");
    assert!(
        matches!(&error, Error::Validation(message) if message.contains("core.timeout_multiplier")),
        "{error}"
    );
    let rows: i64 = home
        .store()
        .conn
        .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 0, "the refused request admits nothing");
}

/// An invalid multiplier is rejected by the hot reload, which keeps the last
/// valid configuration active instead of replacing it.
#[test]
fn invalid_multiplier_is_rejected_by_hot_reload() {
    let home = common::Home::new();
    let service = Service::new(home.path.clone());
    assert!(service.refresh_config().unwrap());
    let path = home.path.join("config.toml");
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(
        &path,
        format!("{original}\n[core]\ndefault_timeout_seconds = 480\ntimeout_multiplier = 0.9\n"),
    )
    .unwrap();
    let error = service.refresh_config().expect_err("sub-one multiplier");
    assert!(
        error.to_string().contains("core.timeout_multiplier"),
        "{error}"
    );
}
