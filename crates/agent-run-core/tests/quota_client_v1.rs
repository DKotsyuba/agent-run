//! Synthetic protocol checks; no real quota service, credentials, or databases are used.
use agent_run_core::quota_client::{
    self, CoverageState, Error, ModelState, ServiceError, UnobservedReason,
};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, path::PathBuf};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
    task::JoinHandle,
};

const REQUEST: &str = include_str!("fixtures/quota-client-v1/snapshot-request-v1.json");
const KNOWN: &str = include_str!("fixtures/quota-client-v1/snapshot-v2.json");
const UNKNOWN: &str = include_str!("fixtures/quota-client-v1/unknown-snapshot-v2.json");
const EXHAUSTED: &str = include_str!("fixtures/quota-client-v1/exhausted-snapshot-v2.json");
const HISTORICAL_V1: &str = include_str!("fixtures/quota-client-v1/snapshot-v1.json");
const OVERFLOW: &str = include_str!("fixtures/quota-client-v1/overflow-snapshot-v2.json");
const UNKNOWN_ACCOUNT: &str = include_str!("fixtures/quota-client-v1/unknown-account-v1.json");

/// Allocates only an owned fixture directory under the configured short socket root.
fn socket_dir() -> TempDir {
    let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    tempfile::Builder::new()
        .prefix("qc-v1-")
        .tempdir_in(base)
        .unwrap()
}

/// Starts a private local fake which records exactly the bytes received from the client.
fn fake(response: Vec<u8>) -> (TempDir, PathBuf, JoinHandle<Vec<u8>>) {
    let dir = socket_dir();
    let path = dir.path().join("quota.sock");
    let listener = UnixListener::bind(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660)).unwrap();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        let _ = stream.write_all(&response).await;
        request
    });
    (dir, path, task)
}

/// Formats a fixture as the service's one-line JSON protocol frame.
fn frame(text: &str) -> Vec<u8> {
    let value: Value = serde_json::from_str(text).unwrap();
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.push(b'\n');
    bytes
}

/// Makes the fixture's scoped read and returns the exact observed request.
async fn exchange(response: Vec<u8>) -> (Result<quota_client::Snapshot, Error>, Vec<u8>) {
    let (_dir, path, mut server) = fake(response);
    let result = quota_client::snapshot(&path, None, &["acct-test".into()], Some("model-a")).await;
    let request = match tokio::time::timeout(std::time::Duration::from_secs(2), &mut server).await {
        Ok(result) => result.unwrap(),
        Err(_) => {
            server.abort();
            let _ = server.await;
            panic!("fake quota server did not finish");
        }
    };
    (result, request)
}

#[tokio::test]
async fn quota_client_v1_preserves_known_unknown_and_exhausted_facts() {
    for (input, expected) in [
        (KNOWN, ModelState::Known),
        (UNKNOWN, ModelState::Unknown),
        (EXHAUSTED, ModelState::Exhausted),
    ] {
        let (result, request) = exchange(frame(input)).await;
        assert_eq!(
            serde_json::from_slice::<Value>(&request).unwrap(),
            serde_json::from_str::<Value>(REQUEST).unwrap()
        );
        let snapshot = result.unwrap();
        assert_eq!(snapshot.accounts[0].models[0].state, expected);
        assert_eq!(snapshot.source_epoch, "00000000000000000000000000000000");
        if expected == ModelState::Unknown {
            assert_eq!(snapshot.coverage.state, CoverageState::Partial);
            assert_eq!(snapshot.accounts[0].models[0].quota_score, None);
            assert_eq!(snapshot.accounts[0].models[0].remaining_percent, None);
        } else {
            let window = &snapshot.accounts[0].windows[0];
            assert_eq!(window.models.as_ref().unwrap(), &["model-a"]);
            assert_eq!(
                window.observed_at,
                Some(if expected == ModelState::Known {
                    10000.0
                } else {
                    10001.0
                })
            );
            assert_eq!(
                window.valid_until,
                Some(if expected == ModelState::Known {
                    10900.0
                } else {
                    10901.0
                })
            );
            assert_eq!(window.last_reset_at, Some(20000.0));
            assert_eq!(window.forecast.key.source, "fixture-source");
        }
        if expected == ModelState::Exhausted {
            assert!(snapshot.accounts[0].latches[0].active);
            assert_eq!(
                snapshot.accounts[0].latches[0].models.as_ref().unwrap(),
                &["model-a"]
            );
            assert_eq!(snapshot.accounts[0].models[0].quota_score, None);
        }
    }
}

#[tokio::test]
async fn quota_client_v1_accepts_real_quota_overflow_fixture() {
    let (_dir, path, server) = fake(frame(OVERFLOW));
    let result = quota_client::snapshot(&path, None, &[], None)
        .await
        .unwrap();
    assert_eq!(result.accounts[0].models[0].state, ModelState::Unknown);
    let window = &result.accounts[0].windows[0];
    assert!(window.forecast.known);
    assert_eq!(window.forecast.remaining_percent, Some(80.0));
    assert!(window.forecast.burn_percent_per_hour.is_none());
    assert!(window.forecast.sustainable_percent_per_hour.is_none());
    assert!(window.evaluation.is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn quota_client_v1_service_failures_are_not_capacity() {
    for (response, expected) in [
        (UNKNOWN_ACCOUNT, ServiceError::UnknownAccount),
        (
            r#"{"version":1,"error":"response_too_large"}"#,
            ServiceError::ResponseTooLarge,
        ),
        (
            r#"{"version":1,"error":"history_too_large"}"#,
            ServiceError::HistoryTooLarge,
        ),
        (
            r#"{"version":1,"error":"secret-invented-error"}"#,
            ServiceError::Other,
        ),
    ] {
        let (result, _) = exchange(frame(response)).await;
        assert!(matches!(result, Err(Error::Service(error)) if error == expected));
        assert!(!format!("{}", Error::Service(expected)).contains("secret"));
    }
}

#[tokio::test]
async fn quota_client_v1_rejects_malformed_truncated_and_oversized_frames() {
    let cases = vec![
        b"{invalid}\n".to_vec(),
        b"{\"version\":1}".to_vec(),
        vec![b'x'; 4 * 1024 * 1024 + 1],
        b"{\"version\":1,\"version\":1,\"error\":\"unknown_account\"}\n".to_vec(),
    ];
    for bytes in cases {
        let (result, _) = exchange(bytes).await;
        assert!(matches!(result, Err(Error::InvalidResponse)));
    }
}

#[tokio::test]
async fn quota_client_v1_ignores_additive_result_fields_but_keeps_envelope_strict() {
    let mut value: Value = serde_json::from_str(KNOWN).unwrap();
    value["result"]["future"] = json!({"optional":"synthetic"});
    value["result"]["coverage"]["future"] = json!(true);
    value["result"]["coverage"]["last_collection"]["sources"][0]["future"] = json!([1, 2]);
    value["result"]["accounts"][0]["future"] = json!(null);
    value["result"]["accounts"][0]["models"][0]["future"] = json!("synthetic");
    value["result"]["accounts"][0]["models"][0]["state_validity"]["future"] = json!("optional");
    value["result"]["accounts"][0]["windows"][0]["future"] = json!({"nested":true});
    value["result"]["accounts"][0]["windows"][0]["forecast"]["future"] = json!(17);
    let (result, _) = exchange(frame(&value.to_string())).await;
    let snapshot = result.unwrap();
    assert_eq!(snapshot.accounts[0].models[0].state, ModelState::Known);
    assert_eq!(snapshot.accounts[0].windows[0].last_reset_at, Some(20000.0));

    for field in ["unexpected", "error"] {
        let mut invalid = value.clone();
        invalid[field] = json!("unknown_account");
        let (result, _) = exchange(frame(&invalid.to_string())).await;
        assert!(matches!(result, Err(Error::InvalidResponse)));
    }

    let mut unknown: Value = serde_json::from_str(UNKNOWN).unwrap();
    unknown["result"]["accounts"][0]["models"][0]["state_validity"]["future"] = json!(true);
    let (result, _) = exchange(frame(&unknown.to_string())).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn quota_client_v1_requires_consistent_schema_v2_state_validity() {
    let (v1, _) = exchange(frame(HISTORICAL_V1)).await;
    assert!(matches!(v1, Err(Error::InvalidResponse)));

    for (input, validity) in [
        (KNOWN, json!({"kind":"unknown"})),
        (KNOWN, json!({"kind":"latched_no_reset"})),
        (KNOWN, json!({"kind":"future_kind"})),
        (KNOWN, json!({"kind":"until"})),
        (KNOWN, json!({"kind":"until","at":-1})),
        (KNOWN, json!({"kind":"until","at":"later"})),
        (UNKNOWN, json!({"kind":"until","at":10900.0})),
        (UNKNOWN, json!({"kind":"latched_no_reset"})),
        (EXHAUSTED, json!({"kind":"unknown"})),
    ] {
        let mut value: Value = serde_json::from_str(input).unwrap();
        value["result"]["accounts"][0]["models"][0]["state_validity"] = validity;
        let (result, _) = exchange(frame(&value.to_string())).await;
        assert!(matches!(result, Err(Error::InvalidResponse)));
    }

    let mut missing: Value = serde_json::from_str(KNOWN).unwrap();
    missing["result"]["accounts"][0]["models"][0]
        .as_object_mut()
        .unwrap()
        .remove("state_validity");
    let (result, _) = exchange(frame(&missing.to_string())).await;
    assert!(matches!(result, Err(Error::InvalidResponse)));

    let raw = String::from_utf8(frame(KNOWN))
        .unwrap()
        .replace("\"at\":10900.0", "\"at\":1e999");
    assert!(raw.contains("\"at\":1e999"));
    let (result, _) = exchange(raw.into_bytes()).await;
    assert!(matches!(result, Err(Error::InvalidResponse)));

    let mut boundary: Value = serde_json::from_str(KNOWN).unwrap();
    boundary["result"]["accounts"][0]["models"][0]["state_validity"]["at"] = json!(10000.0);
    let (result, _) = exchange(frame(&boundary.to_string())).await;
    assert!(result.is_ok());

    let mut open_latch: Value = serde_json::from_str(EXHAUSTED).unwrap();
    let account = &mut open_latch["result"]["accounts"][0];
    account["models"][0]["state_validity"] = json!({"kind":"latched_no_reset","future":true});
    account["models"][0]["reset_at"] = Value::Null;
    account["models"][0]["exhausted_until"] = Value::Null;
    account["latches"][0]["reset_at"] = Value::Null;
    let (result, _) = exchange(frame(&open_latch.to_string())).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn quota_client_v1_rejects_invalid_schema_scope_states_and_missing_nulls() {
    let original: Value = serde_json::from_str(KNOWN).unwrap();
    for (pointer, replacement) in [
        ("/version", json!(2)),
        ("/result/schema_version", json!(1)),
        ("/result/schema_version", json!(3)),
        ("/result/source_epoch", json!("invalid")),
        ("/result/config_revision", json!("invalid")),
        ("/result/revision", json!(-1)),
        ("/result/coverage/state", json!("new-state")),
        ("/result/accounts/0/account", json!("other-account")),
        ("/result/accounts/0/models/0/state", json!("new-state")),
        ("/result/accounts/0/models/0/quota_score", Value::Null),
        (
            "/result/accounts/0/models/0/limiting_window/source",
            json!("unreturned-source"),
        ),
        (
            "/result/accounts/0/windows/0/last_remaining_percent",
            json!(101),
        ),
        (
            "/result/accounts/0/windows/0/forecast/key/source",
            json!("other-source"),
        ),
    ] {
        let mut altered = original.clone();
        *altered.pointer_mut(pointer).unwrap() = replacement;
        let (result, _) = exchange(frame(&altered.to_string())).await;
        assert!(
            matches!(result, Err(Error::InvalidResponse)),
            "case: {pointer}"
        );
    }
    let mut altered = original;
    altered
        .pointer_mut("/result/accounts/0/models/0")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .remove("burn_percent_per_hour");
    let (result, _) = exchange(frame(&altered.to_string())).await;
    assert!(matches!(result, Err(Error::InvalidResponse)));

    let mut missing_reset: Value = serde_json::from_str(KNOWN).unwrap();
    missing_reset["result"]["accounts"][0]["windows"][0]
        .as_object_mut()
        .unwrap()
        .remove("last_reset_at");
    let (result, _) = exchange(frame(&missing_reset.to_string())).await;
    assert!(matches!(result, Err(Error::InvalidResponse)));

    let mut contradictory: Value = serde_json::from_str(KNOWN).unwrap();
    let forecast = &mut contradictory["result"]["accounts"][0]["windows"][0]["forecast"];
    forecast["known"] = json!(false);
    forecast["remaining_percent"] = Value::Null;
    forecast["risk"] = json!("unknown");
    contradictory["result"]["accounts"][0]["windows"][0]["evaluation"] = Value::Null;
    let (result, _) = exchange(frame(&contradictory.to_string())).await;
    assert!(matches!(result, Err(Error::InvalidResponse)));

    let mut contradictory: Value = serde_json::from_str(UNKNOWN).unwrap();
    contradictory["result"]["accounts"][0]["models"][0]["quota_score"] = json!(0);
    let (result, _) = exchange(frame(&contradictory.to_string())).await;
    assert!(matches!(result, Err(Error::InvalidResponse)));

    let mut wrong_model: Value = serde_json::from_str(KNOWN).unwrap();
    wrong_model["result"]["accounts"][0]["models"][0]["model"] = json!("other-alias");
    wrong_model["result"]["accounts"][0]["models"][0]["native_model"] = json!("other-native");
    let (result, _) = exchange(frame(&wrong_model.to_string())).await;
    assert!(matches!(result, Err(Error::InvalidResponse)));

    let mut native_match: Value = serde_json::from_str(KNOWN).unwrap();
    native_match["result"]["accounts"][0]["models"][0]["model"] = json!("other-alias");
    let (result, _) = exchange(frame(&native_match.to_string())).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn quota_client_v1_preserves_case_sensitive_provider_identities() {
    let mut value: Value = serde_json::from_str(KNOWN).unwrap();
    let template = value["result"]["accounts"][0]["models"][0].clone();
    let aliases: Vec<Value> = ["Claude", "claude", "1gateway"]
        .into_iter()
        .map(|provider| {
            let mut model = template.clone();
            model["provider"] = json!(provider);
            model
        })
        .collect();
    value["result"]["accounts"][0]["models"] = json!(aliases);
    value["result"]["accounts"][0]["windows"][0]["runtime"] = json!("Claude");
    value["result"]["coverage"]["state"] = json!("partial");
    value["result"]["coverage"]["last_collection"]["sources"][0]["provider"] = json!("1gateway");
    value["result"]["coverage"]["unobserved"] = json!([
        {"account":"acct-test", "provider":"claude", "reason":"limits_source_none"}
    ]);
    let (result, _) = exchange(frame(&value.to_string())).await;
    let snapshot = result.unwrap();
    let providers: Vec<_> = snapshot.accounts[0]
        .models
        .iter()
        .map(|m| m.provider.as_str())
        .collect();
    assert_eq!(providers, ["Claude", "claude", "1gateway"]);
    assert_eq!(snapshot.accounts[0].windows[0].runtime, "Claude");
    assert_eq!(snapshot.coverage.unobserved[0].provider, "claude");
    assert_eq!(
        snapshot.coverage.last_collection.unwrap().sources[0].provider,
        "1gateway"
    );

    for pointer in [
        "/result/accounts/0/models/0/provider",
        "/result/accounts/0/windows/0/runtime",
        "/result/coverage/unobserved/0/provider",
        "/result/coverage/last_collection/sources/0/provider",
    ] {
        for invalid in ["_invalid".to_owned(), "a".repeat(65), "ä".to_owned()] {
            let mut bad = value.clone();
            *bad.pointer_mut(pointer).unwrap() = json!(invalid);
            let (result, _) = exchange(frame(&bad.to_string())).await;
            assert!(
                matches!(result, Err(Error::InvalidResponse)),
                "case: {pointer}"
            );
        }
    }
}

#[tokio::test]
async fn quota_client_v1_preserves_empty_configuration_and_nullable_membership() {
    let mut empty: Value = serde_json::from_str(UNKNOWN).unwrap();
    empty["result"]["accounts"] = json!([]);
    empty["result"]["revision"] = Value::Null;
    empty["result"]["coverage"] =
        json!({"state":"unknown", "unobserved":[], "last_collection":null});
    let (_dir, path, server) = fake(frame(&empty.to_string()));
    let result = quota_client::snapshot(&path, None, &[], None)
        .await
        .unwrap();
    assert!(result.accounts.is_empty());
    assert_eq!(result.revision, None);
    assert_eq!(result.coverage.state, CoverageState::Unknown);
    server.await.unwrap();

    let mut unknown_membership: Value = serde_json::from_str(KNOWN).unwrap();
    unknown_membership["result"]["accounts"][0]["windows"][0]["models"] = Value::Null;
    unknown_membership["result"]["accounts"][0]["models"][0]["native_model"] = json!("primary");
    let (result, _) = exchange(frame(&unknown_membership.to_string())).await;
    assert!(result.unwrap().accounts[0].windows[0].models.is_none());

    let mut no_reset: Value = serde_json::from_str(KNOWN).unwrap();
    let account = &mut no_reset["result"]["accounts"][0];
    account["models"][0]["reset_at"] = Value::Null;
    account["windows"][0]["last_reset_at"] = Value::Null;
    account["windows"][0]["forecast"]["reset_at"] = Value::Null;
    account["windows"][0]["evaluation"]["reset_at"] = Value::Null;
    account["windows"][0]["evaluation"]["marker"] = json!("no_reset");
    account["advice"][0]["reset_at"] = Value::Null;
    let (result, _) = exchange(frame(&no_reset.to_string())).await;
    assert!(
        result.unwrap().accounts[0].windows[0]
            .last_reset_at
            .is_none()
    );
}

#[tokio::test]
async fn quota_client_v1_keeps_exhaustion_when_forecast_is_stale() {
    let mut stale: Value = serde_json::from_str(EXHAUSTED).unwrap();
    stale["result"]["evaluated_at"] = json!(11000.0);
    let account = &mut stale["result"]["accounts"][0];
    account["models"][0]["remaining_percent"] = Value::Null;
    account["models"][0]["burn_percent_per_hour"] = Value::Null;
    account["models"][0]["risk"] = json!("unknown");
    let forecast = &mut account["windows"][0]["forecast"];
    forecast["known"] = json!(false);
    for name in [
        "remaining_percent",
        "reset_at",
        "observed_at",
        "burn_percent_per_hour",
        "sustainable_percent_per_hour",
        "burn_span_seconds",
    ] {
        forecast[name] = Value::Null;
    }
    forecast["warmup"] = json!(true);
    forecast["risk"] = json!("unknown");
    account["windows"][0]["evaluation"] = Value::Null;
    let advice = &mut account["advice"][0];
    advice["known"] = json!(false);
    advice["remaining_percent"] = Value::Null;
    advice["reset_at"] = Value::Null;
    advice["warmup"] = json!(true);
    advice["risk"] = json!("unknown");
    advice["recommendations"] = json!([]);
    let (result, _) = exchange(frame(&stale.to_string())).await;
    let snapshot = result.unwrap();
    assert_eq!(snapshot.accounts[0].models[0].state, ModelState::Exhausted);
    assert_eq!(snapshot.accounts[0].models[0].remaining_percent, None);
    assert!(snapshot.accounts[0].latches[0].active);
    assert!(!snapshot.accounts[0].windows[0].forecast.known);
    assert_eq!(snapshot.accounts[0].windows[0].forecast.reset_at, None);
    assert_eq!(
        snapshot.accounts[0].windows[0].last_remaining_percent,
        Some(0.0)
    );
    assert_eq!(snapshot.accounts[0].windows[0].last_reset_at, Some(20000.0));
}

#[tokio::test]
async fn quota_client_v1_rejects_invalid_requests_before_connecting() {
    let nonexistent = std::path::Path::new("/no-such-quota-socket");
    let cases = [
        (vec!["valid".to_owned(); 257], None),
        (vec!["not valid".to_owned()], None),
        (vec!["Account".to_owned()], None),
        (vec!["1account".to_owned()], None),
        (vec!["a".repeat(97)], None),
        (vec![], Some("m".repeat(257))),
        (vec![], Some("contains\ncontrol".to_owned())),
    ];
    for (accounts, model) in cases {
        assert!(matches!(
            quota_client::snapshot(nonexistent, None, &accounts, model.as_deref()).await,
            Err(Error::InvalidRequest)
        ));
    }
}

#[tokio::test]
async fn quota_client_v1_rejects_symlink_and_wrong_socket_owner() {
    let (_dir, path, server) = fake(frame(KNOWN));
    let link = path.with_extension("link");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(matches!(
        quota_client::snapshot(&link, None, &[], None).await,
        Err(Error::UnsafeSocket)
    ));
    // SAFETY: geteuid has no arguments and only reads the process identity.
    let other_uid = unsafe { libc::geteuid() } ^ 1;
    assert!(matches!(
        quota_client::snapshot(&path, Some(other_uid), &[], None).await,
        Err(Error::UnsafeSocket)
    ));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn quota_client_v1_allows_parent_symlinks_and_os_authorized_permissions() {
    let (dir, path, server) = fake(frame(KNOWN));
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).unwrap();
    let alias = dir.path().join("directory-alias");
    std::os::unix::fs::symlink(dir.path(), &alias).unwrap();
    let result = quota_client::snapshot(
        &alias.join("quota.sock"),
        None,
        &["acct-test".into()],
        Some("model-a"),
    )
    .await;
    assert!(result.is_ok());
    server.await.unwrap();
}

#[tokio::test]
async fn quota_client_v1_does_not_wait_for_eof_after_response() {
    let dir = socket_dir();
    let path = dir.path().join("quota.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        stream.write_all(&frame(KNOWN)).await.unwrap();
        std::future::pending::<()>().await;
    });
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        quota_client::snapshot(&path, None, &["acct-test".into()], Some("model-a")),
    )
    .await;
    assert!(matches!(result, Ok(Ok(_))));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn quota_client_v1_uses_one_absolute_timeout() {
    let dir = socket_dir();
    let path = dir.path().join("quota.sock");
    let listener = UnixListener::bind(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        stream.write_all(b"{\"version\":1").await.unwrap();
        std::future::pending::<()>().await;
    });
    let started = std::time::Instant::now();
    assert!(matches!(
        quota_client::snapshot(&path, None, &[], None).await,
        Err(Error::Timeout)
    ));
    assert!(started.elapsed() >= std::time::Duration::from_secs(5));
    assert!(started.elapsed() < std::time::Duration::from_secs(8));
    server.abort();
    let _ = server.await;
}

/// Uses only an explicitly prepared synthetic Quota service and its known configuration.
#[tokio::test]
#[ignore = "requires an explicitly prepared synthetic Quota service"]
async fn quota_client_v1_synthetic_service_interop() {
    let socket = PathBuf::from(std::env::var_os("QUOTA_INTEROP_SOCKET").expect("socket required"));
    let uid = std::env::var("QUOTA_INTEROP_UID")
        .expect("service UID required")
        .parse::<u32>()
        .expect("invalid service UID");
    let account = std::env::var("QUOTA_INTEROP_ACCOUNT").expect("account required");
    let provider = std::env::var("QUOTA_INTEROP_PROVIDER").expect("provider required");
    let model = std::env::var("QUOTA_INTEROP_MODEL").expect("model required");
    let native_model = std::env::var("QUOTA_INTEROP_NATIVE_MODEL").expect("native model required");
    let config_revision =
        std::env::var("QUOTA_INTEROP_CONFIG_REVISION").expect("config revision required");

    let snapshot = quota_client::snapshot(
        &socket,
        Some(uid),
        std::slice::from_ref(&account),
        Some(&model),
    )
    .await
    .expect("synthetic quota snapshot failed");
    assert!(snapshot.config_revision == config_revision);
    assert_eq!(snapshot.accounts.len(), 1);
    let result = &snapshot.accounts[0];
    assert!(result.account == account);
    assert_eq!(result.models.len(), 1);
    let binding = &result.models[0];
    assert!(
        binding.provider == provider
            && binding.model == model
            && binding.native_model == native_model
    );
    assert_eq!(binding.state, ModelState::Unknown);
    assert!(
        binding.quota_score.is_none()
            && binding.limiting_window.is_none()
            && binding.remaining_percent.is_none()
    );
    assert!(matches!(
        snapshot.coverage.state,
        CoverageState::Unknown | CoverageState::Partial
    ));
    assert!(
        snapshot
            .coverage
            .unobserved
            .iter()
            .any(|source| source.account == account
                && source.provider == provider
                && source.reason == UnobservedReason::LimitsSourceNone)
    );
}
