//! Admission and hot reload retain concurrency controls without execution expiry.
mod common;
use agent_run_core::service::Service;
use serde_json::json;

/// Admissions expose no persisted execution lifetime columns or request fields.
#[test]
fn admission_does_not_persist_execution_lifetime() {
    let home = common::Home::new();
    let (id, created) = home
        .store()
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    assert!(created);
    let store = home.store();
    let columns: Vec<String> = store
        .conn
        .prepare("SELECT name FROM pragma_table_info('agents')")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    for key in ["timeout_seconds", "warned", "silent_seconds"] {
        assert!(!columns.iter().any(|column| column == key));
    }
    assert!(
        serde_json::to_value(store.get(&id).unwrap().request)
            .unwrap()
            .get("timeout_seconds")
            .is_none()
    );
}

/// A removed lifetime key cannot hot-reload or replace the last valid config.
#[test]
fn retired_policy_is_rejected_by_hot_reload() {
    let home = common::Home::new();
    let service = Service::new(home.path.clone());
    assert!(service.refresh_config().unwrap());
    let path = home.path.join("config.toml");
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(
        &path,
        format!("{original}\n[core]\ntimeout_multiplier = 1.2\n"),
    )
    .unwrap();
    assert!(service.refresh_config().is_err());
    std::fs::write(&path, original).unwrap();
    assert!(
        !service.refresh_config().unwrap(),
        "invalid revision never became effective"
    );
}
