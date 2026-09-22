//! Account-bound quota observation normalization, persistence, and latch.

use agent_run_core::capacity::quota::{normalize_collector_output, CollectorScope};
use agent_run_domain::catalog::AccountId;
use agent_run_store::quota::record_quota_snapshot;
use serde_json::json;
use std::{collections::BTreeSet, path::Path, str::FromStr};
use tempfile::tempdir;

/// The global account every fixture binds to; labels never appear here.
fn account() -> AccountId {
    AccountId::from_str("acct-main").unwrap()
}

/// One configured GLM-like scope with two explicit models.
fn scope() -> CollectorScope {
    CollectorScope {
        runtime: "glm".into(),
        source: "lua:abc123".into(),
        models: BTreeSet::from(["glm-4.7".to_owned(), "glm-4.6".to_owned()]),
    }
}

/// Normalizes one raw collector round for `account`.
fn normalize(
    raw: &serde_json::Value,
) -> agent_run_domain::Result<agent_run_domain::catalog::NormalizedQuotaSnapshot> {
    normalize_collector_output(&account(), &scope(), raw, 256, 256)
}

#[test]
fn normalizes_valid_output_into_one_physical_pool_per_lane() {
    let snapshot = normalize(&json!({
        "windows": [
            {"pool":"primary","window":"five_hour","models":["glm-4.7","glm-4.6"],
             "remaining_percent":42.5,"reset_at":2000.0,"observed_at":1000.0},
            {"pool":"primary","window":"monthly_mcp","models":["glm-4.7"],"observed_at":1000.0}
        ]
    }))
    .unwrap();
    snapshot.validate().unwrap();
    // Both models share one account-bound physical key regardless of provider alias.
    assert_eq!(snapshot.models.len(), 2);
    for model in &snapshot.models {
        assert_eq!(model.pools[0].key.as_str(), "acct-main::primary");
    }
    let glm = snapshot
        .models
        .iter()
        .find(|m| m.model == "glm-4.7")
        .unwrap();
    let windows = &glm.pools[0].windows;
    assert_eq!(windows.len(), 2);
    let unknown = windows.iter().find(|w| w.name == "monthly_mcp").unwrap();
    // Unknown remaining stays absent and validity defaults to the TTL.
    assert_eq!(unknown.remaining_percent, None);
    assert_eq!(unknown.reset_at, None);
    assert_eq!(unknown.valid_until, 1900.0);
}

#[test]
fn rejects_foreign_models_duplicates_bad_numbers_and_overflow() {
    let foreign = normalize(&json!({
        "windows": [{"pool":"primary","window":"five_hour","models":["gpt-5.1"],
                     "remaining_percent":10.0,"observed_at":1000.0}]
    }));
    assert!(foreign.is_err());
    let duplicate = normalize(&json!({
        "windows": [
            {"pool":"primary","window":"five_hour","models":["glm-4.7"],"remaining_percent":10.0,"observed_at":1000.0},
            {"pool":"primary","window":"five_hour","models":["glm-4.7"],"remaining_percent":20.0,"observed_at":1000.0}
        ]
    }));
    assert!(duplicate.is_err());
    let out_of_range = normalize(&json!({
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":101.0,"observed_at":1000.0}]
    }));
    assert!(out_of_range.is_err());
    let not_a_number = normalize(&json!({
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":"42","observed_at":1000.0}]
    }));
    assert!(not_a_number.is_err());
    let inverted_time = normalize(&json!({
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":10.0,"observed_at":1000.0,"valid_until":999.0}]
    }));
    assert!(inverted_time.is_err());
    let extra_field = normalize(&json!({
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":10.0,"observed_at":1000.0,"weekly":true}]
    }));
    assert!(extra_field.is_err());
    let overflow: serde_json::Value = json!({"windows": (0..257).map(|i| json!({"pool":"p","window":format!("w{i}"),"models":["glm-4.7"],"observed_at":1.0}).to_owned()).collect::<Vec<_>>()});
    assert!(normalize(&overflow).is_err());
}

/// Reads the newest persisted row for one lane and account.
fn latest(home: &Path) -> (Option<f64>, Option<f64>, f64) {
    let store = agent_run_store::Store::open(home).unwrap();
    store
        .conn
        .query_row(
            "SELECT remaining_percent,reset_at,valid_until FROM capacity_samples \
             WHERE lane='primary' AND target='acct-main' ORDER BY id DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
}

#[test]
fn exhaustion_latch_survives_unknown_and_releases_on_reset_or_evidence() {
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    let exhausted = normalize(&json!({
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":0.0,"reset_at":2000.0,"observed_at":1000.0}]
    }))
    .unwrap();
    let r1 = record_quota_snapshot(home.path(), "glm", &exhausted, 100, 1500.0).unwrap();
    assert_eq!(r1, 1);

    // A round that only produced unknown data must not displace the exhausted fact.
    let unknown = normalize(&json!({
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],"observed_at":1500.0}]
    }))
    .unwrap();
    let r2 = record_quota_snapshot(home.path(), "glm", &unknown, 100, 1600.0).unwrap();
    assert_eq!(r2, r1 + 1);
    let (remaining, reset, valid_until) = latest(home.path());
    assert_eq!(remaining, Some(0.0));
    assert_eq!(reset, Some(2000.0));
    assert!(valid_until >= 2000.0, "latch extends survival to the reset");

    // Positive fresh evidence releases the latch immediately.
    let fresh = normalize(&json!({
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":60.0,"reset_at":3000.0,"observed_at":1700.0}]
    }))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &fresh, 100, 1700.0).unwrap();
    assert_eq!(latest(home.path()).0, Some(60.0));

    // Re-latch, then let the known reset pass: unknown data supersedes again.
    record_quota_snapshot(home.path(), "glm", &exhausted, 100, 1800.0).unwrap();
    record_quota_snapshot(home.path(), "glm", &unknown, 100, 2500.0).unwrap();
    assert_eq!(latest(home.path()).0, None);
}

#[test]
fn malformed_rounds_and_noop_rounds_leave_history_and_revision_untouched() {
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    let good = normalize(&json!({
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":50.0,"observed_at":1000.0}]
    }))
    .unwrap();
    let r1 = record_quota_snapshot(home.path(), "glm", &good, 100, 1000.0).unwrap();
    // A timeout or malformed output never reaches persistence at all.
    assert!(normalize(&json!({"windows": "garbage"})).is_err());
    assert!(normalize(&json!({})).is_err());
    // A structurally valid round with no windows mutates nothing.
    let empty = normalize(&json!({"windows": []})).unwrap();
    let r2 = record_quota_snapshot(home.path(), "glm", &empty, 100, 1100.0).unwrap();
    assert_eq!(r1, r2);
    let store = agent_run_store::Store::open(home.path()).unwrap();
    let rows: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM capacity_samples", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(rows, 1);
    // Membership evidence travels with the row.
    let payload: String = store
        .conn
        .query_row("SELECT payload_json FROM capacity_samples", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert!(payload.contains("glm-4.7"));
}
