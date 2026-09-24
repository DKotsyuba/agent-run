//! Account-bound quota observation normalization, persistence, and latch.

use agent_run_core::capacity::quota::{normalize_collector_output, CollectorScope};
use agent_run_domain::catalog::{AccountId, AccountRecord, AccountStatus};
use agent_run_store::quota::record_quota_snapshot;
use serde_json::{json, Value};
use std::{collections::BTreeSet, path::Path, str::FromStr};
use tempfile::tempdir;

/// The global account every fixture binds to; labels never appear here.
fn account() -> AccountId {
    AccountId::from_str("acct-main").unwrap()
}

/// Registers the fixture account so persisted facts satisfy the registry FK,
/// returning the capacity revision that registration committed.
fn registered(home: &std::path::Path) -> i64 {
    let mut store = agent_run_store::Store::open(home).unwrap();
    store
        .register_account(&AccountRecord {
            account_id: account(),
            auth_family: "openai".parse().unwrap(),
            secret_ref: "native:codex".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    store.quota_capacity_revision().unwrap()
}

/// One configured GLM-like scope with two explicit models and a stable
/// collector identity that does not move with script revisions.
fn scope() -> CollectorScope {
    CollectorScope {
        runtime: "glm".into(),
        source: "glm-native".into(),
        models: BTreeSet::from(["glm-4.7".to_owned(), "glm-4.6".to_owned()]),
    }
}

/// Normalizes one raw collector round for `account` observed by host time `at`.
fn normalize_at(
    raw: &Value,
    at: f64,
) -> agent_run_domain::Result<agent_run_domain::catalog::NormalizedQuotaSnapshot> {
    normalize_collector_output(&account(), &scope(), raw, at, 256, 256)
}

/// Normalizes against a host clock after every fixture observation.
fn normalize(
    raw: &Value,
) -> agent_run_domain::Result<agent_run_domain::catalog::NormalizedQuotaSnapshot> {
    normalize_at(raw, 3000.0)
}

#[test]
fn normalizes_valid_output_into_one_physical_pool_per_lane() {
    let snapshot = normalize(&json!({
        "version": 1,
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
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["gpt-5.1"],
                     "remaining_percent":10.0,"observed_at":1000.0}]
    }));
    assert!(foreign.is_err());
    let duplicate = normalize(&json!({
        "version": 1,
        "windows": [
            {"pool":"primary","window":"five_hour","models":["glm-4.7"],"remaining_percent":10.0,"observed_at":1000.0},
            {"pool":"primary","window":"five_hour","models":["glm-4.7"],"remaining_percent":20.0,"observed_at":1000.0}
        ]
    }));
    assert!(duplicate.is_err());
    let repeated_model = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7","glm-4.7"],
                     "remaining_percent":10.0,"observed_at":1000.0}]
    }));
    assert!(repeated_model.is_err());
    let out_of_range = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":101.0,"observed_at":1000.0}]
    }));
    assert!(out_of_range.is_err());
    let not_a_number = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":"42","observed_at":1000.0}]
    }));
    assert!(not_a_number.is_err());
    let inverted_time = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":10.0,"observed_at":1000.0,"valid_until":999.0}]
    }));
    assert!(inverted_time.is_err());
    let extra_field = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":10.0,"observed_at":1000.0,"weekly":true}]
    }));
    assert!(extra_field.is_err());
    let overflow: Value = json!({"version": 1, "windows": (0..257).map(|i| json!({"pool":"p","window":format!("w{i}"),"models":["glm-4.7"],"observed_at":1.0}).to_owned()).collect::<Vec<_>>()});
    assert!(normalize(&overflow).is_err());
}

#[test]
fn enforces_explicit_version_envelope_and_host_time_boundary() {
    // Missing version, wrong version, and foreign top-level keys all fail.
    assert!(normalize(&json!({"windows": []})).is_err());
    assert!(normalize(&json!({"version": 2, "windows": []})).is_err());
    assert!(normalize(&json!({"version": 1, "windows": [], "extra": true})).is_err());
    // An observation later than the host clock is a future claim, not a fact.
    let future = normalize_at(
        &json!({
            "version": 1,
            "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                         "remaining_percent":10.0,"observed_at":999.0}]
        }),
        500.0,
    );
    assert!(future.is_err());
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
    let base = registered(home.path());
    let exhausted = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":0.0,"reset_at":2000.0,"observed_at":1000.0}]
    }))
    .unwrap();
    let r1 = record_quota_snapshot(home.path(), "glm", &exhausted, 100, 1500.0).unwrap();
    assert_eq!(r1, base + 1);
    assert_eq!(
        latch_rows(home.path()),
        1,
        "fresh exhaustion latches durably"
    );

    // A round that only produced unknown data must not displace the exhausted fact.
    let unknown = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],"observed_at":1500.0}]
    }))
    .unwrap();
    let r2 = record_quota_snapshot(home.path(), "glm", &unknown, 100, 1600.0).unwrap();
    assert_eq!(r2, r1 + 1);
    let (remaining, reset, valid_until) = latest(home.path());
    assert_eq!(remaining, Some(0.0));
    assert_eq!(reset, Some(2000.0));
    assert!(valid_until >= 2000.0, "latch extends survival to the reset");
    assert_eq!(latch_rows(home.path()), 1, "unknown round keeps the latch");

    // A stale positive observation never releases the latch.
    let stale = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":60.0,"observed_at":1000.0,"valid_until":1100.0}]
    }))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &stale, 100, 1600.0).unwrap();
    assert_eq!(latest(home.path()).0, Some(0.0));
    assert_eq!(
        latch_rows(home.path()),
        1,
        "stale positive never clears the latch"
    );

    // Positive fresh evidence releases the latch immediately.
    let fresh = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":60.0,"reset_at":3000.0,"observed_at":1700.0}]
    }))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &fresh, 100, 1700.0).unwrap();
    assert_eq!(latest(home.path()).0, Some(60.0));
    assert_eq!(
        latch_rows(home.path()),
        0,
        "fresh positive clears the latch"
    );

    // Re-latch, then let the known reset pass: unknown data supersedes again
    // without synthesizing unobserved capacity.
    record_quota_snapshot(home.path(), "glm", &exhausted, 100, 1800.0).unwrap();
    record_quota_snapshot(home.path(), "glm", &unknown, 100, 2500.0).unwrap();
    assert_eq!(latest(home.path()).0, None);
    assert_eq!(latch_rows(home.path()), 0, "expired reset clears the latch");
}

/// Counts durable latch rows for the fixture account.
fn latch_rows(home: &std::path::Path) -> i64 {
    let store = agent_run_store::Store::open(home).unwrap();
    store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM quota_exhaustion WHERE account_id='acct-main'",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

/// A no-data round cannot clear durable exhaustion or advance its revision.
#[test]
fn empty_observation_keeps_durable_exhaustion() {
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    registered(home.path());
    let exhausted = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":0.0,"observed_at":1500.0,"valid_until":2500.0}]
    }))
    .unwrap();
    let revision = record_quota_snapshot(home.path(), "glm", &exhausted, 100, 1600.0).unwrap();
    let empty = normalize(&json!({"version":1,"windows":[]})).unwrap();
    let after = record_quota_snapshot(home.path(), "glm", &empty, 100, 1700.0).unwrap();
    assert_eq!(
        latch_rows(home.path()),
        1,
        "absence is not positive evidence"
    );
    assert_eq!(after, revision, "no-data round changes no quota facts");
}

/// An older positive sample cannot supersede a newer authoritative exhaustion,
/// even while the older sample's own validity horizon has not expired.
#[test]
fn older_valid_observation_cannot_clear_newer_exhaustion() {
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    registered(home.path());
    let exhausted = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":0.0,"observed_at":1500.0,"valid_until":2500.0}]
    }))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &exhausted, 100, 1600.0).unwrap();
    let older = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":60.0,"observed_at":1400.0,"valid_until":2500.0}]
    }))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &older, 100, 1700.0).unwrap();
    assert_eq!(
        latch_rows(home.path()),
        1,
        "newer exhausted evidence must win"
    );
}

/// An unrelated pool cannot release a latch for a missing physical pool.
#[test]
fn omitted_pool_keeps_exhaustion() {
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    registered(home.path());
    let exhausted = normalize(&json!({"version":1,"windows":[
        {"pool":"primary","window":"five_hour","models":["glm-4.7"],
         "remaining_percent":0.0,"observed_at":1500.0,"valid_until":2500.0}
    ]}))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &exhausted, 100, 1600.0).unwrap();
    let unrelated = normalize(&json!({"version":1,"windows":[
        {"pool":"secondary","window":"five_hour","models":["glm-4.7"],
         "remaining_percent":90.0,"observed_at":1700.0}
    ]}))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &unrelated, 100, 1700.0).unwrap();
    assert_eq!(latch_rows(home.path()), 1);
}

/// A delayed exhausted report cannot replace the newer latched observation.
#[test]
fn older_exhaustion_cannot_roll_back_latch() {
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    registered(home.path());
    let newer = normalize(&json!({"version":1,"windows":[
        {"pool":"primary","window":"five_hour","models":["glm-4.7"],
         "remaining_percent":0.0,"observed_at":1500.0,"reset_at":2500.0}
    ]}))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &newer, 100, 1600.0).unwrap();
    let older = normalize(&json!({"version":1,"windows":[
        {"pool":"primary","window":"five_hour","models":["glm-4.7"],
         "remaining_percent":0.0,"observed_at":1400.0,"reset_at":2400.0}
    ]}))
    .unwrap();
    let revision = record_quota_snapshot(home.path(), "glm", &older, 100, 1700.0).unwrap();
    let store = agent_run_store::Store::open(home.path()).unwrap();
    let fact: (f64, Option<f64>) = store
        .conn
        .query_row(
            "SELECT observed_at,reset_at FROM quota_exhaustion WHERE account_id='acct-main'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(fact, (1500.0, Some(2500.0)));
    assert_eq!(latch_rows(home.path()), 1);
    let empty = normalize(&json!({"version":1,"windows":[]})).unwrap();
    let cleared = record_quota_snapshot(home.path(), "glm", &empty, 100, 2600.0).unwrap();
    assert_eq!(cleared, revision + 1, "expired reset advances the revision");
    assert_eq!(latch_rows(home.path()), 0);
    assert_eq!(
        record_quota_snapshot(home.path(), "glm", &empty, 100, 2700.0).unwrap(),
        cleared
    );
    record_quota_snapshot(home.path(), "glm", &older, 100, 2800.0).unwrap();
    assert_eq!(
        latch_rows(home.path()),
        0,
        "expired old zero cannot relatch"
    );
}

#[test]
fn persistence_requires_a_registered_account() {
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    let snapshot = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":50.0,"observed_at":1000.0}]
    }))
    .unwrap();
    let err = record_quota_snapshot(home.path(), "glm", &snapshot, 100, 1000.0).unwrap_err();
    assert!(err.to_string().contains("not registered"));
    registered(home.path());
    assert!(record_quota_snapshot(home.path(), "glm", &snapshot, 100, 1000.0).is_ok());
}

#[test]
fn exhaustion_latch_survives_a_collector_source_change() {
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    registered(home.path());
    let exhausted = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":0.0,"reset_at":2000.0,"observed_at":1000.0}]
    }))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &exhausted, 100, 1500.0).unwrap();
    // A script revision changes the collector source identity; the latched
    // physical fact for the lane and window still governs.
    let mut replaced = scope();
    replaced.source = "glm-native-v2".into();
    let unknown = normalize_collector_output(
        &account(),
        &replaced,
        &json!({"version": 1, "windows": [{"pool":"primary","window":"five_hour",
                 "models":["glm-4.7"],"observed_at":1600.0}]}),
        3000.0,
        256,
        256,
    )
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &unknown, 100, 1600.0).unwrap();
    assert_eq!(latest(home.path()).0, Some(0.0));
}

#[test]
fn shared_pool_windows_persist_once_with_full_membership() {
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    let base = registered(home.path());
    let shared = normalize(&json!({
        "version": 1,
        "windows": [
            {"pool":"primary","window":"five_hour","models":["glm-4.7","glm-4.6"],
             "remaining_percent":42.5,"observed_at":1000.0},
            {"pool":"primary","window":"monthly_mcp","models":["glm-4.7"],
             "remaining_percent":90.0,"observed_at":1000.0}
        ]
    }))
    .unwrap();
    let revision = record_quota_snapshot(home.path(), "glm", &shared, 100, 1500.0).unwrap();
    assert_eq!(revision, base + 1);
    let store = agent_run_store::Store::open(home.path()).unwrap();
    let rows: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM capacity_samples", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(rows, 2, "one row per physical window, not per model");
    let five_hour: String = store
        .conn
        .query_row(
            "SELECT payload_json FROM capacity_samples WHERE window='five_hour'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(five_hour.contains("glm-4.7") && five_hour.contains("glm-4.6"));
    let (account_id, quota_key): (String, String) = store
        .conn
        .query_row(
            "SELECT account_id,quota_key FROM capacity_samples WHERE window='five_hour'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(account_id, "acct-main");
    assert_eq!(quota_key, "acct-main::primary");
}

#[test]
fn malformed_rounds_and_noop_rounds_leave_history_and_revision_untouched() {
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    registered(home.path());
    let good = normalize(&json!({
        "version": 1,
        "windows": [{"pool":"primary","window":"five_hour","models":["glm-4.7"],
                     "remaining_percent":50.0,"observed_at":1000.0}]
    }))
    .unwrap();
    let r1 = record_quota_snapshot(home.path(), "glm", &good, 100, 1000.0).unwrap();
    // A timeout or malformed output never reaches persistence at all.
    assert!(normalize(&json!({"version": 1, "windows": "garbage"})).is_err());
    assert!(normalize(&json!({})).is_err());
    // A structurally valid round with no windows mutates nothing.
    let empty = normalize(&json!({"version": 1, "windows": []})).unwrap();
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

/// A native-signal latch (`Store::latch_native_exhaustion`) follows the same
/// release rules as collector latches: an older positive collector sample
/// never clears it, a newer authoritative positive observation of the same
/// lane and window does.
#[test]
fn native_signal_latch_survives_stale_positive_and_releases_on_fresh() {
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    registered(home.path());
    let mut store = agent_run_store::Store::open(home.path()).unwrap();
    store
        .latch_native_exhaustion(
            &account(),
            "glm",
            "primary",
            "five_hour",
            "claude-rate-limit-event",
            &BTreeSet::from(["glm-4.7".to_owned(), "glm-4.6".to_owned()]),
            1600.0,
            None,
        )
        .unwrap();
    assert_eq!(latch_rows(home.path()), 1);
    let positive = |observed: f64, at: f64| {
        normalize_at(
            &json!({"version":1,"windows":[{"pool":"primary","window":"five_hour","models":["glm-4.7","glm-4.6"],
                "remaining_percent":80.0,"observed_at":observed}]}),
            at,
        )
        .unwrap()
    };
    record_quota_snapshot(home.path(), "glm", &positive(1500.0, 1700.0), 100, 1700.0).unwrap();
    assert_eq!(
        latch_rows(home.path()),
        1,
        "an older positive sample never clears it"
    );
    record_quota_snapshot(home.path(), "glm", &positive(1650.0, 1700.0), 100, 1700.0).unwrap();
    assert_eq!(
        latch_rows(home.path()),
        0,
        "a newer positive observation releases it"
    );
}

/// One account, pool and window name observed by two sources, as after a
/// collector source switch with a carried latch: `s1` governs only
/// `glm-4.7`, `s2` only `glm-4.6`. Each persisted physical row claims only
/// its own source's members; neither inherits the other's model.
#[test]
fn same_window_name_from_two_sources_keeps_separate_membership() {
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    registered(home.path());
    let window = |source: &str, remaining: f64| {
        json!({"source": source, "name": "five_hour", "remaining_percent": remaining,
               "reset_at": 2000.0, "observed_at": 1000.0, "valid_until": 1900.0})
    };
    let snapshot: agent_run_domain::catalog::NormalizedQuotaSnapshot =
        serde_json::from_value(json!({
            "account": "acct-main",
            "models": [
                {"model": "glm-4.6", "pools": [{"key": "acct-main::primary", "windows": [window("s2", 50.0)]}]},
                {"model": "glm-4.7", "pools": [{"key": "acct-main::primary", "windows": [window("s1", 0.0)]}]}
            ]
        }))
        .unwrap();
    snapshot.validate().unwrap();
    record_quota_snapshot(home.path(), "glm", &snapshot, 100, 1500.0).unwrap();
    let store = agent_run_store::Store::open(home.path()).unwrap();
    let members = |source: &str| -> Value {
        let payload: String = store
            .conn
            .query_row(
                "SELECT payload_json FROM capacity_samples WHERE window='five_hour' AND source=?",
                [source],
                |row| row.get(0),
            )
            .unwrap();
        serde_json::from_str::<Value>(&payload).unwrap()["models"].clone()
    };
    assert_eq!(members("s1"), json!(["glm-4.7"]));
    assert_eq!(members("s2"), json!(["glm-4.6"]));
}

/// A partial unknown round cannot narrow the model set of a carried physical
/// exhaustion latch; unrelated pool and source rows keep their own members.
#[test]
fn partial_unknown_keeps_all_exhausted_window_members() {
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    registered(home.path());
    let first = normalize(&json!({"version":1,"windows":[
        {"pool":"primary","window":"five_hour","models":["glm-4.7","glm-4.6"],
         "remaining_percent":0.0,"reset_at":2500.0,"observed_at":1000.0},
        {"pool":"secondary","window":"five_hour","models":["glm-4.7"],
         "remaining_percent":40.0,"reset_at":2500.0,"observed_at":1000.0}
    ]}))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &first, 100, 1500.0).unwrap();
    let partial = normalize(&json!({"version":1,"windows":[
        {"pool":"primary","window":"five_hour","models":["glm-4.7"],"observed_at":1600.0}
    ]}))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &partial, 100, 1600.0).unwrap();
    let store = agent_run_store::Store::open(home.path()).unwrap();
    let members: String = store
        .conn
        .query_row(
            "SELECT payload_json FROM capacity_samples WHERE quota_key='acct-main::primary' \
         AND source='glm-native' AND window='five_hour' ORDER BY observed_at DESC,id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&members).unwrap()["models"],
        json!(["glm-4.6", "glm-4.7"])
    );
    assert_eq!(latch_rows(home.path()), 1);
    let definitions = serde_json::from_value(json!([{
        "id":"fixture-provider","harness":"codex","connection":{"kind":"native"},
        "auth_family":"openai","limits_source":"codex_appserver",
        "models":[{"id":"fixture-b","native_model":"glm-4.6"}],
        "bindings":[{"label":"main","account":"acct-main"}]
    }]))
    .unwrap();
    let catalog = agent_run_domain::catalog::ProviderCatalog::new(
        store.list_accounts().unwrap(),
        definitions,
    )
    .unwrap();
    let blocked = agent_run_core::capacity::provider_ranking::provider_candidates_at(
        &store,
        &catalog,
        &"fixture-provider".parse().unwrap(),
        "fixture-b",
        None,
        &BTreeSet::new(),
        1600.0,
    )
    .unwrap_err();
    assert!(
        matches!(
            blocked,
            agent_run_domain::Error::QuotaAdmission(
                agent_run_domain::catalog::QuotaAdmissionError::QuotaExhausted { .. }
            )
        ),
        "{blocked:?}"
    );
}

/// A disjoint unknown observation cannot replace the only sample carrying
/// an unsettled latch's model membership or make that model admissible.
#[test]
fn disjoint_unknown_keeps_exhausted_model_blocked() {
    use agent_run_core::capacity::provider_ranking::provider_candidates_at;
    use agent_run_domain::catalog::{ProviderCatalog, QuotaAdmissionError};
    let home = tempdir().unwrap();
    agent_run_store::Store::initialize(home.path()).unwrap();
    registered(home.path());
    let exhausted = normalize(&json!({"version":1,"windows":[
        {"pool":"primary","window":"five_hour","models":["glm-4.7"],
         "remaining_percent":0.0,"reset_at":2500.0,"observed_at":1000.0}
    ]}))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &exhausted, 100, 1500.0).unwrap();
    let disjoint = normalize(&json!({"version":1,"windows":[
        {"pool":"primary","window":"five_hour","models":["glm-4.6"],"observed_at":1600.0}
    ]}))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &disjoint, 100, 1600.0).unwrap();
    let store = agent_run_store::Store::open(home.path()).unwrap();
    let members: String = store
        .conn
        .query_row(
            "SELECT payload_json FROM capacity_samples WHERE quota_key='acct-main::primary' \
         AND source='glm-native' AND window='five_hour' ORDER BY observed_at DESC,id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&members).unwrap()["models"],
        json!(["glm-4.7"])
    );
    assert_eq!(latch_rows(home.path()), 1);
    let definitions = serde_json::from_value(json!([{
        "id":"fixture-provider","harness":"codex","connection":{"kind":"native"},
        "auth_family":"openai","limits_source":"codex_appserver",
        "models":[{"id":"fixture-a","native_model":"glm-4.7"},
                  {"id":"fixture-b","native_model":"glm-4.6"}],
        "bindings":[{"label":"main","account":"acct-main"}]
    }]))
    .unwrap();
    let catalog = ProviderCatalog::new(store.list_accounts().unwrap(), definitions).unwrap();
    let candidate = |model| {
        provider_candidates_at(
            &store,
            &catalog,
            &"fixture-provider".parse().unwrap(),
            model,
            None,
            &BTreeSet::new(),
            1600.0,
        )
    };
    assert!(matches!(
        candidate("fixture-a"),
        Err(agent_run_domain::Error::QuotaAdmission(
            QuotaAdmissionError::QuotaExhausted { .. }
        ))
    ));
    let other = candidate("fixture-b").unwrap();
    assert!(!other.candidates[0].quota_known);
    assert!(other.candidates[0].physical_keys.is_empty());

    let recovered = normalize(&json!({"version":1,"windows":[
        {"pool":"primary","window":"five_hour","models":["glm-4.7"],
         "remaining_percent":80.0,"reset_at":3000.0,"observed_at":1700.0}
    ]}))
    .unwrap();
    record_quota_snapshot(home.path(), "glm", &recovered, 100, 1700.0).unwrap();
    assert_eq!(latch_rows(home.path()), 0);
    let available = provider_candidates_at(
        &store,
        &catalog,
        &"fixture-provider".parse().unwrap(),
        "fixture-a",
        None,
        &BTreeSet::new(),
        1700.0,
    )
    .unwrap();
    assert!(available.candidates[0].quota_known);
}
