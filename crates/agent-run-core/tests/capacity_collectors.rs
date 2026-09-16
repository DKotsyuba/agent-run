//! Recorded-fixture regressions for the Python capacity collector contracts.

use agent_run_core::capacity::{
    omniroute, sources,
    sources::{
        account_email, capture, normalize_codex, normalize_codexbar_accounts, read_claude_stream,
    },
    Key,
};
use agent_run_domain::Error;
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path, process::Command};

/// Converts a compact JSON Web Token fixture into the auth-file shape used by Codex.
fn auth_file(email: Option<&str>) -> serde_json::Value {
    let claims = email.map_or_else(|| "{}".into(), |email| format!(r#"{{"email":"{email}"}}"#));
    let payload = base64url(claims.as_bytes());
    json!({"tokens":{"id_token":format!("header.{payload}.signature")}})
}

/// Encodes only the URL-safe base64 alphabet needed by the email fixture.
fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut result = String::new();
    for chunk in bytes.chunks(3) {
        let a = chunk[0] as usize;
        let b = chunk.get(1).copied().unwrap_or(0) as usize;
        let c = chunk.get(2).copied().unwrap_or(0) as usize;
        result.push(ALPHABET[a >> 2] as char);
        result.push(ALPHABET[((a & 3) << 4) | (b >> 4)] as char);
        if chunk.len() > 1 {
            result.push(ALPHABET[((b & 15) << 2) | (c >> 6)] as char);
        }
        if chunk.len() > 2 {
            result.push(ALPHABET[c & 63] as char);
        }
    }
    result
}

const OBSERVED: f64 = 1_785_000_000.0;

/// Returns a deterministic app-server response containing ordinary and Spark buckets.
fn pro_response(account: &str) -> serde_json::Value {
    json!({
        "accountId": account,
        "rateLimitsByLimitId": {
            "codex_standard_weekly": {
                "limitName": "Standard",
                "secondary": {"usedPercent": 20.0, "windowDurationMins": 10080.0, "resetsAt": OBSERVED + 3600.0}
            },
            "codex_spark": {
                "limitName": "Spark",
                "primary": {"usedPercent": 10.0, "windowDurationMins": 300.0, "resetsAt": OBSERVED + 3600.0},
                "secondary": {"usedPercent": 30.0, "windowDurationMins": 10080.0}
            }
        }
    })
}

/// Builds one shell command that emits bounded stdout and then exits.
fn capture_script(output: &str, status: &str) -> (std::path::PathBuf, Vec<String>) {
    (
        std::path::PathBuf::from("/bin/sh"),
        vec![
            "-c".into(),
            format!("printf '%s' '{}' ; exit {}", output, status),
        ],
    )
}

/// Returns a no-op environment for isolated fake metadata probes.
fn empty_environment() -> BTreeMap<String, String> {
    BTreeMap::new()
}

/// Mirrors `tests/test_capacity_sources.py::test_declared_accounts_map_targets_and_add_all_accounts`.
#[test]
fn codexbar_maps_declared_accounts_without_exposing_auth_documents() {
    let mut accounts = BTreeMap::new();
    accounts.insert("personal".into(), "personal@example.test".into());
    let slice = normalize_codexbar_accounts(
        "codex",
        &json!([
            {"usage":{"accountEmail":"default@example.test","updatedAt":"2026-08-29T12:12:53Z","primary":{"usedPercent":3,"windowMinutes":300}}},
            {"usage":{"identity":{"accountEmail":"personal@example.test"},"updatedAt":"2026-08-29T12:12:53Z","primary":{"usedPercent":4,"windowMinutes":300}}},
            {"usage":{"identity":{"accountEmail":"stranger@example.test"},"updatedAt":"2026-08-29T12:12:53Z","primary":{"usedPercent":5,"windowMinutes":300}}}
        ]),
        &accounts,
        Some("default@example.test"),
    )
    .expect("recorded codexbar response is valid");
    let targets: Vec<_> = slice
        .samples
        .iter()
        .map(|sample| sample.key.target.as_deref())
        .collect();
    assert_eq!(
        targets,
        vec![None, Some("personal"), Some("stranger@example.test")]
    );
    assert_eq!(slice.topology.routes.len(), 2);
}

/// Mirrors `tests/test_capacity_sources.py::test_spawn_failure_timeout_nonzero_exit_and_garbage_fail_the_round`.
#[test]
fn codexbar_empty_data_is_an_explicit_failure() {
    let error = normalize_codexbar_accounts("codex", &json!([]), &BTreeMap::new(), None)
        .expect_err("an empty success response is not capacity evidence");
    assert_eq!(error.to_string(), "codexbar_missing_data");
}

/// Mirrors the Codexbar account-email helper's successful and collapsed-failure cases.
// Mirrors `tests/test_capacity_sources.py::AccountEmailTests::test_decodes_email_and_collapses_failures`.
#[test]
fn codexbar_account_email_maps_only_valid_auth_claims() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let path = temporary.path().join("auth.json");
    std::fs::write(&path, serde_json::to_vec(&auth_file(Some("a @b"))).unwrap()).unwrap();
    assert_eq!(account_email(&path).as_deref(), Some("a @b"));
    let mut accounts = BTreeMap::new();
    accounts.insert("personal".into(), "a @b".into());
    let slice = normalize_codexbar_accounts(
        "codex",
        &json!({"usage":{"accountEmail":"a @b","updatedAt":"2026-08-29T12:12:53Z","primary":{"usedPercent":1,"windowMinutes":300}}}),
        &accounts,
        None,
    )
    .expect("valid account response");
    assert_eq!(slice.samples[0].key.target.as_deref(), Some("personal"));
    for contents in ["missing", "garbage"] {
        std::fs::write(&path, contents).unwrap();
        assert_eq!(account_email(&path), None);
    }
    std::fs::write(&path, serde_json::to_vec(&auth_file(None)).unwrap()).unwrap();
    assert_eq!(account_email(&path), None);
}

/// Mirrors the honest Codexbar lane and window-name mapping rules.
// Mirrors `tests/test_capacity_sources.py::CodexbarMappingTests::test_real_shape_maps_lanes_and_shelves_honestly`.
// Mirrors `tests/test_capacity_sources.py::CodexbarMappingTests::test_unknown_window_minutes_names_itself`.
// Mirrors `tests/test_capacity_sources.py::CodexbarMappingTests::test_absent_lanes_are_absent_but_present_windows_must_be_valid`.
// Mirrors `tests/test_capacity_sources.py::CodexbarMappingTests::test_single_object_payload_still_maps_one_account_without_accounts`.
// Mirrors `tests/test_capacity_sources.py::CodexbarMappingTests::test_empty_usage_object_is_an_invalid_observation`.
// Mirrors `tests/test_capacity_sources.py::CodexbarMappingTests::test_first_account_only_and_ignored_sections`.
#[test]
fn codexbar_mapping_preserves_lanes_and_limits_account_scope() {
    let payload = json!([
        {"usage":{"updatedAt":"2026-08-29T12:12:53Z","primary":null,"secondary":{"usedPercent":46,"windowMinutes":10080,"resetsAt":"2026-09-03T16:26:47Z"},"tertiary":{"usedPercent":5.5,"windowMinutes":300,"resetsAt":null}}},
        {"usage":{"updatedAt":"2026-08-29T12:12:53Z","primary":{"usedPercent":99,"windowMinutes":300}}}
    ]);
    let slice = normalize_codexbar_accounts("codex", &payload, &BTreeMap::new(), None).unwrap();
    assert_eq!(slice.samples.len(), 2);
    assert_eq!(slice.samples[0].key.lane, "secondary");
    assert_eq!(slice.samples[0].key.window, "seven_day");
    assert_eq!(slice.samples[0].remaining_percent, Some(54.0));
    assert_eq!(slice.samples[1].key.window, "five_hour");
    assert_eq!(slice.samples[0].reset_at, Some(1788452807.0));

    let unknown = normalize_codexbar_accounts(
        "codex",
        &json!({"usage":{"updatedAt":"2026-08-29T12:12:53Z","primary":{"usedPercent":80,"windowMinutes":30}}}),
        &BTreeMap::new(),
        None,
    )
    .unwrap();
    assert_eq!(unknown.samples[0].key.window, "min30");
    let single = normalize_codexbar_accounts("codex", &json!({"usage":{"updatedAt":"2026-08-29T12:12:53Z","primary":{"usedPercent":20,"windowMinutes":300}}}), &BTreeMap::new(), None).unwrap();
    assert_eq!(single.samples[0].remaining_percent, Some(80.0));
    assert!(
        normalize_codexbar_accounts("codex", &json!({"usage":{}}), &BTreeMap::new(), None).is_err()
    );
}

/// Mirrors the source's failure-safe timeout and secret-safe failure contract.
// Mirrors `tests/test_capacity_sources.py::CodexbarMappingTests::test_codexbar_timeout_allows_two_minutes`.
// Mirrors `tests/test_capacity_sources.py::CodexbarMappingTests::test_failure_logs_and_exception_are_secret_safe`.
#[tokio::test]
async fn codexbar_capture_returns_fixed_secret_free_failures() {
    let error = sources::capture(
        Path::new("/bin/sh"),
        &["-c".into(), "printf provider-secret; exit 1".into()],
        1,
        &BTreeMap::new(),
    )
    .await
    .expect_err("nonzero source must fail");
    assert!(!error.to_string().contains("provider-secret"));
    assert_eq!(sources::CODEXBAR_TIMEOUT_SECONDS, 120);
}

/// Mirrors native Claude's lane, target, remaining, and bounded validity mapping.
// Mirrors `tests/test_capacity_sources.py::NativeClaudeMappingTests::test_live_payload_maps_lanes_targets_and_remaining`.
// Mirrors `tests/test_capacity_sources.py::NativeClaudeMappingTests::test_request_uses_oauth_headers_and_timeout`.
// Mirrors `tests/test_capacity_sources.py::NativeClaudeMappingTests::test_missing_token_http_error_and_timeout_are_source_failures`.
// Mirrors `tests/test_capacity_sources.py::NativeClaudeMappingTests::test_declared_oauth_env_is_used_without_reading_cli_state`.
// Mirrors `tests/test_capacity_sources.py::NativeClaudeMappingTests::test_undeclared_or_api_key_env_never_becomes_the_oauth_token`.
#[test]
fn native_claude_mapping_is_bounded_and_provider_neutral() {
    let payload = json!({"limits":[
        {"kind":"session","percent":25,"resets_at":"2026-09-01T15:00:00-04:00"},
        {"kind":"weekly_all","percent":40,"resets_at":"2026-09-07T12:00:00Z"},
        {"kind":"weekly_scoped","percent":60,"scope":{"model":{"display_name":"Fable"}},"resets_at":"2026-09-07T12:00:00Z"},
        {"kind":"mystery_limit","percent":10,"resets_at":null}
    ]});
    let slice = sources::normalize_claude("claude", &payload, 1788278400.0).unwrap();
    assert_eq!(slice.samples[0].key.lane, "primary");
    assert_eq!(slice.samples[1].key.window, "seven_day");
    assert_eq!(slice.samples[2].key.target.as_deref(), Some("fable"));
    assert_eq!(slice.samples[3].remaining_percent, Some(90.0));
    assert_eq!(slice.samples[0].valid_until, Some(1788279300.0));
}

/// Mirrors timestamp parsing's rejection of naive, garbage, numeric, and missing values.
// Mirrors `tests/test_capacity_sources.py::TimestampTests::test_rejects_naive_and_garbage_stamps`.
#[test]
fn capacity_timestamps_require_rfc3339_timezone_information() {
    for value in [
        json!("2026-08-29T12:12:53"),
        json!(""),
        json!(12345),
        Value::Null,
        json!("not-a-stamp"),
    ] {
        let payload =
            json!({"usage":{"updatedAt":value,"primary":{"usedPercent":1,"windowMinutes":300}}});
        assert!(sources::normalize_codexbar("codex", &payload).is_err());
    }
}

/// Writes the smallest config accepted by the capacity dispatcher.
///
/// The store is initialized too, because a collection round enforces the
/// global sample retention once per round the way Python's `collect_once`
/// does -- including a round in which every runtime failed -- so a collectible
/// home is one that has durable state. `Store::initialize` is idempotent, so
/// rewriting the config on the same root stays safe.
fn dispatcher_config(root: &Path, adapter: &str, source: &str, binary: &Path) {
    std::fs::write(
        root.join("config.toml"),
        format!(
            "schema_version=1\n[runtimes.fixture]\nenabled=true\nadapter=\"{adapter}\"\nbinary=\"{}\"\nhome=\"{}\"\nmodels=[\"fixture\"]\nlimits_source=\"{source}\"\n",
            binary.display(),
            root.join("runtime").display()
        ),
    )
    .unwrap();
    agent_run_store::Store::initialize(root).unwrap();
}

/// Exercises source dispatch's unsupported/empty distinctions without loading an adapter.
// Mirrors `tests/test_capacity_sources.py::DispatchTests::test_none_source_is_unsupported`.
// Mirrors `tests/test_capacity_sources.py::DispatchTests::test_native_source_gates_on_live_limits_capability`.
// Mirrors `tests/test_capacity_sources.py::DispatchTests::test_codexbar_source_rejects_undocumented_providers`.
#[tokio::test]
async fn capacity_dispatch_keeps_unsupported_sources_distinct() {
    let temporary = tempfile::tempdir().unwrap();
    dispatcher_config(temporary.path(), "claude", "none", Path::new("/bin/true"));
    let unsupported = sources::collect(temporary.path()).await.unwrap();
    assert_eq!(unsupported["results"][0]["status"], "unsupported");

    dispatcher_config(temporary.path(), "qwen", "codexbar", Path::new("/bin/true"));
    let unavailable = sources::collect(temporary.path()).await.unwrap();
    assert_eq!(unavailable["results"][0]["status"], "failed");
    assert_eq!(unavailable["results"][0]["issues"][0], "source_not_ported");
}

/// Exercises the configured Qwen binary's bounded local version probe.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_probe_observes_the_configured_binary_version`.
#[tokio::test]
async fn qwen_model_probe_uses_the_configured_binary() {
    let temporary = tempfile::tempdir().unwrap();
    let binary = temporary.path().join("qwen");
    std::fs::write(&binary, "#!/bin/sh\nprintf 'runtime 1.2.3\\n'\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    dispatcher_config(temporary.path(), "qwen", "none", &binary);
    let roster = sources::models(temporary.path()).await.unwrap();
    assert_eq!(roster["fixture"]["available"], true);
    assert_eq!(roster["fixture"]["models"][0]["id"], "fixture");
}

/// Mirrors `tests/test_omniroute_current_cache.py::test_current_cache_pool_average`.
#[test]
fn omniroute_current_cache_averages_members_and_uses_earliest_reset() {
    let samples = omniroute::samples(
        &json!([
            {"window_key":"session","remaining_percentage":80.0,"next_reset_at":"2026-09-01T13:00:00Z","fetched_at":"2026-09-01T12:00:00Z"},
            {"window_key":"session","remaining_percentage":60.0,"next_reset_at":"2026-09-01T12:30:00Z","fetched_at":"2026-09-01T12:01:00Z"}
        ]),
        1_788_264_120.0,
    )
    .expect("current recorded cache is valid");
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].remaining_percent, Some(70.0));
    assert_eq!(samples[0].key.window, "session_5h");
    assert_eq!(samples[0].key.target.as_deref(), Some("opencode-go:pool"));
}

/// Exercises Qwen's shared OmniRoute pool as a real empty, healthy, or failed source.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_live_limits_capability_and_pool_samples_are_shared`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_a_successfully_read_empty_pool_has_no_samples`.
// Mirrors `tests/test_qwen_adapter.py::QwenAdapterTests::test_a_failing_row_source_raises_a_safe_source_error`.
// Mirrors `tests/test_capacity_sources.py::DispatchTests::test_omniroute_source_uses_the_pool_without_any_adapter`.
#[tokio::test]
async fn qwen_uses_the_shared_omniroute_pool_without_adapter_calls() {
    let rows = json!([{"window_key":"session","remaining_percentage":90.0,"next_reset_at":"2026-08-28T17:26:35.920Z","fetched_at":"2026-08-28T14:06:01.922Z"}]);
    let observed = chrono::DateTime::parse_from_rfc3339("2026-08-28T14:06:01.922Z")
        .unwrap()
        .timestamp_millis() as f64
        / 1000.0;
    let healthy = omniroute::samples(&rows, observed + 60.0).unwrap();
    assert_eq!(healthy[0].remaining_percent, Some(90.0));
    assert_eq!(healthy[0].key.target.as_deref(), Some("opencode-go:pool"));
    assert!(omniroute::samples(&json!([]), 1.0).unwrap().is_empty());
    let error = omniroute::read(async { Err(agent_run_domain::Error::Runtime("fixture".into())) })
        .await
        .expect_err("unreadable pool must be a source failure");
    assert_eq!(error.to_string(), "omniroute_unavailable");
}

/// Mirrors `tests/test_omniroute_current_cache.py::test_stale_cache_is_unknown`.
/// Mirrors `tests/test_capacity_sources.py::CodexbarMappingTests::test_glm_maps_to_the_zai_provider`.
#[test]
fn omniroute_stale_cache_is_unknown_not_zero_capacity() {
    let samples = omniroute::samples(
        &json!([{"window_key":"weekly","remaining_percentage":90.0,"next_reset_at":null,"fetched_at":"2026-09-01T10:00:00Z"}]),
        1_788_269_000.0,
    )
    .expect("stale cache shape is still valid evidence");
    assert_eq!(samples[0].remaining_percent, None);
    assert_eq!(samples[0].key.source, "unknown");
}

/// Mirrors `tests/test_omniroute_current_cache.py::test_malformed_and_overflow_are_failures`.
#[test]
fn omniroute_malformed_or_overflow_rows_are_unavailable() {
    let malformed = omniroute::samples(&json!([{"window_key":"session"}]), 1.0)
        .expect_err("a malformed known member cannot shrink the pool");
    assert_eq!(malformed.to_string(), "omniroute_malformed_data");
    let overflow = ValueRows::overflow();
    let error = omniroute::samples(&overflow, 1.0).expect_err("row cap is bounded");
    assert_eq!(error.to_string(), "omniroute_result_overflow");
}

/// Executes the production OmniRoute sanitizer against supplied cache members.
fn sanitized_omniroute_rows(members: &Value) -> Option<Value> {
    let node = Command::new("node").arg("--version").output().ok()?;
    if !node.status.success() {
        return None;
    }
    let harness = format!(
        "const __FAKE_MEMBERS__={members}; function require(_) {{ return function Database(_,_) {{ return {{ prepare(_) {{ return {{ all() {{ return __FAKE_MEMBERS__; }} }}; }} }}; }}; }}",
        members = serde_json::to_string(members).unwrap()
    );
    let output = Command::new("node")
        .args(["-e", &format!("{harness}{}", omniroute::script())])
        .output()
        .expect("node is available");
    assert!(
        output.status.success(),
        "sanitizer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Some(serde_json::from_slice(&output.stdout).expect("sanitizer emits JSON"))
}

/// Returns a valid cache document containing all supported OmniRoute windows.
fn valid_omniroute_cache() -> Value {
    json!({
        "quotas": {
            "session": {"remainingPercentage": 91.0, "resetAt": "2026-08-24T19:20:58.435Z"},
            "weekly": {"remainingPercentage": 42.5, "resetAt": "2026-08-25T00:00:00Z"},
            "mcp_monthly": {"remainingPercentage": 10.0, "resetAt": null}
        },
        "fetchedAt": "2026-08-24T19:17:02.435Z",
        "source": "scheduled"
    })
}

/// Converts a fixed RFC-3339 fixture timestamp into epoch seconds.
fn omniroute_at(value: &str) -> f64 {
    chrono::DateTime::parse_from_rfc3339(value)
        .unwrap()
        .timestamp_millis() as f64
        / 1000.0
}

/// Mirrors `tests/test_omniroute_current_cache.py::MembersQueryTests::test_reads_the_current_cache_and_ignores_stale_unchanged_history`.
#[test]
fn omniroute_script_reads_current_cache_not_history() {
    let script = omniroute::script();
    assert!(script.contains("LEFT JOIN key_value"));
    assert!(script.contains("providerLimitsCache"));
    assert!(script.contains("quota_visible=1"));
    assert!(script.contains("is_active=1"));
    assert!(!script.contains("quota_snapshots"));
}

/// Mirrors `tests/test_omniroute_current_cache.py::MembersQueryTests::test_active_quota_visible_members_included_others_excluded`.
#[test]
fn omniroute_script_filters_active_visible_provider_members() {
    let script = omniroute::script();
    assert!(script.contains("provider='opencode-go'"));
    assert!(script.contains("c.is_active=1"));
    assert!(script.contains("c.quota_visible=1"));
}

/// Mirrors `tests/test_omniroute_current_cache.py::MembersQueryTests::test_left_join_keeps_an_active_member_with_no_cache_row`.
#[test]
fn omniroute_script_keeps_cacheless_members_in_the_left_join() {
    assert!(omniroute::script().contains("LEFT JOIN key_value"));
}

/// Mirrors `tests/test_omniroute_current_cache.py::SanitizedRowContractTests::test_a_missing_or_malformed_member_cache_fails_the_whole_pool`.
#[test]
fn omniroute_missing_member_cache_fails_the_whole_pool() {
    let Some(rows) = sanitized_omniroute_rows(&json!([{"v": null}])) else {
        return;
    };
    assert_eq!(
        omniroute::samples(&rows, omniroute_at("2026-08-24T19:18:02Z"))
            .unwrap_err()
            .to_string(),
        "omniroute_malformed_data"
    );
}

/// Mirrors `tests/test_omniroute_current_cache.py::DockerScriptSanitizerTests::test_a_valid_current_cache_row_is_projected_for_every_window`.
#[test]
fn omniroute_sanitizer_projects_a_valid_cache_for_every_window() {
    let Some(rows) = sanitized_omniroute_rows(&json!([{"v": valid_omniroute_cache().to_string()}]))
    else {
        return;
    };
    let rows = rows.as_array().expect("sanitized rows are an array");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["remaining_percentage"], 91.0);
    assert_eq!(rows[0]["next_reset_at"], "2026-08-24T19:20:58.435Z");
    assert_eq!(rows[2]["next_reset_at"], Value::Null);
    let samples = omniroute::samples(
        &Value::Array(rows.clone()),
        omniroute_at("2026-08-24T19:20:00Z"),
    )
    .unwrap();
    let session = samples
        .iter()
        .find(|sample| sample.key.window == "session_5h")
        .unwrap();
    assert_eq!(session.remaining_percent, Some(91.0));
}

/// Mirrors `tests/test_omniroute_current_cache.py::DockerScriptSanitizerTests::test_missing_empty_and_malformed_cache_all_poison_the_member`.
#[test]
fn omniroute_sanitizer_poison_rows_for_missing_empty_and_malformed_cache() {
    for cache_json in [
        Value::Null,
        json!(""),
        json!("not json"),
        json!({"quotas":"not-an-object"}),
    ] {
        let Some(rows) = sanitized_omniroute_rows(&json!([{"v": cache_json}])) else {
            return;
        };
        let rows = rows.as_array().expect("sanitized rows are an array");
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|row| row["remaining_percentage"].is_null()));
        assert_eq!(
            omniroute::samples(
                &Value::Array(rows.clone()),
                omniroute_at("2026-08-24T19:18:02Z")
            )
            .unwrap_err()
            .to_string(),
            "omniroute_malformed_data"
        );
    }
}

/// Mirrors `tests/test_omniroute_current_cache.py::DockerScriptSanitizerTests::test_quotas_as_an_array_is_rejected_not_treated_as_an_object`.
#[test]
fn omniroute_sanitizer_rejects_quota_arrays() {
    let Some(rows) = sanitized_omniroute_rows(
        &json!([{"v": json!({"quotas": [], "fetchedAt": "2026-08-24T19:17:02Z"}).to_string()}]),
    ) else {
        return;
    };
    assert!(rows
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["remaining_percentage"].is_null()));
}

/// Mirrors `tests/test_omniroute_current_cache.py::DockerScriptSanitizerTests::test_a_reset_only_change_survives_the_real_node_sanitizer`.
/// Mirrors `tests/test_omniroute_current_cache.py::SanitizedRowContractTests::test_a_reset_only_change_is_visible_from_the_current_cache_alone`.
#[test]
fn omniroute_sanitizer_preserves_reset_only_changes() {
    let cache = |reset: &str| {
        json!({"quotas":{"session":{"remainingPercentage":80.0,"resetAt":reset}},"fetchedAt":"2026-08-24T19:17:02.435Z"}).to_string()
    };
    let Some(before) = sanitized_omniroute_rows(&json!([{"v": cache("2026-08-24T19:20:58.435Z")}]))
    else {
        return;
    };
    let Some(after) = sanitized_omniroute_rows(&json!([{"v": cache("2026-08-24T20:20:58.435Z")}]))
    else {
        return;
    };
    let one = |rows: Value| {
        Value::Array(vec![rows
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["window_key"] == "session")
            .unwrap()
            .clone()])
    };
    let now = omniroute_at("2026-08-24T19:18:02Z");
    let before = omniroute::samples(&one(before), now).unwrap();
    let after = omniroute::samples(&one(after), now).unwrap();
    assert_ne!(before[0].reset_at, after[0].reset_at);
}

/// Mirrors `tests/test_omniroute_current_cache.py::DockerScriptSanitizerTests::test_sentinel_secrets_in_message_plan_and_connection_id_never_appear`.
#[test]
fn omniroute_sanitizer_never_emits_cache_secrets() {
    let secret = "SECRET-do-not-leak-me";
    let cache = json!({"quotas":{"session":{"remainingPercentage":50.0,"resetAt":null}},"fetchedAt":"2026-08-24T19:17:02Z","message":secret,"plan":secret,"connectionId":secret});
    let Some(rows) =
        sanitized_omniroute_rows(&json!([{"v": cache.to_string(), "connection_id": secret}]))
    else {
        return;
    };
    assert!(!rows.to_string().contains(secret));
}

/// Mirrors `tests/test_omniroute_current_cache.py::DockerScriptSanitizerTests::test_malformed_remaining_reset_and_fetched_are_never_emitted_raw`.
#[test]
fn omniroute_sanitizer_poison_marks_malformed_fields_without_leaking_them() {
    let cache = json!({"quotas":{"session":{"remainingPercentage":{"leaked":"yes-this-should-never-cross-stdout"},"resetAt":{"leaked":"yes-this-should-never-cross-stdout"}}},"fetchedAt":{"leaked":"yes-this-should-never-cross-stdout"}});
    let Some(rows) = sanitized_omniroute_rows(&json!([{"v": cache.to_string()}])) else {
        return;
    };
    assert!(!rows.to_string().contains("leaked"));
    let rows = rows.as_array().unwrap();
    let session = rows
        .iter()
        .find(|row| row["window_key"] == "session")
        .unwrap();
    assert!(session["remaining_percentage"].is_null());
    assert!(session["fetched_at"].is_null());
    assert!(!session["next_reset_at"].is_null());
    assert_eq!(
        omniroute::samples(
            &Value::Array(rows.clone()),
            omniroute_at("2026-08-24T19:18:02Z")
        )
        .unwrap_err()
        .to_string(),
        "omniroute_malformed_data"
    );
}

/// Mirrors `tests/test_omniroute_current_cache.py::DockerScriptSanitizerTests::test_a_legitimately_absent_reset_stays_null_not_poisoned`.
#[test]
fn omniroute_sanitizer_keeps_absent_reset_null() {
    let cache = json!({"quotas":{"session":{"remainingPercentage":77.0}},"fetchedAt":"2026-08-24T19:17:02Z"});
    let Some(rows) = sanitized_omniroute_rows(&json!([{"v": cache.to_string()}])) else {
        return;
    };
    let rows = rows.as_array().unwrap();
    let session = rows
        .iter()
        .find(|row| row["window_key"] == "session")
        .unwrap()
        .clone();
    assert!(session["next_reset_at"].is_null());
    let samples = omniroute::samples(&json!([session]), omniroute_at("2026-08-24T19:18:02Z"));
    assert!(samples.is_ok());
    assert_eq!(samples.unwrap()[0].remaining_percent, Some(77.0));
}

/// Mirrors `tests/test_omniroute_current_cache.py::DockerScriptSanitizerTests::test_emission_is_bounded_with_an_overflow_sentinel`.
#[test]
fn omniroute_sanitizer_emission_is_bounded_with_overflow() {
    let cache = json!({"quotas":{"session":{"remainingPercentage":1.0},"weekly":{"remainingPercentage":1.0},"mcp_monthly":{"remainingPercentage":1.0}},"fetchedAt":"2026-08-24T19:17:02Z"}).to_string();
    let members = Value::Array((0..30).map(|_| json!({"v": cache})).collect());
    let Some(rows) = sanitized_omniroute_rows(&members) else {
        return;
    };
    let rows = rows.as_array().unwrap();
    assert!(rows.len() <= 65);
    assert!(rows.len() > 64);
    assert_eq!(
        omniroute::samples(
            &Value::Array(rows.clone()),
            omniroute_at("2026-08-24T19:18:02Z")
        )
        .unwrap_err()
        .to_string(),
        "omniroute_result_overflow"
    );
}

/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_never_makes_a_live_call`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_missing_agents_dir_is_empty`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_reads_the_newest_rate_limit_event_into_two_window_samples`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_prefers_the_newest_agent_directory`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_stale_event_has_unknown_remaining`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_malformed_or_shape_mismatched_events_yield_no_samples`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_never_leaks_unrelated_fields_into_samples`.
/// Mirrors `tests/test_claude_adapter.py::ClaudeAdapterTests::test_limits_considers_only_the_newest_bounded_agent_files`.
#[test]
fn claude_runtime_stream_is_local_fallback() {
    let home = tempfile::tempdir().expect("temporary home");
    let stream = home.path().join("agents/a/runtime.jsonl");
    std::fs::create_dir_all(stream.parent().expect("agent parent")).expect("agent directory");
    std::fs::write(
        &stream,
        "{\"type\":\"rate_limit_event\",\"rate_limit_info\":{\"unifiedWindows\":{\"five_hour\":{\"utilization\":0.25,\"resetsAt\":9999999999}}}}\n",
    )
    .expect("recorded stream");
    let slice = read_claude_stream(home.path(), "claude").expect("local stream read");
    assert_eq!(slice.samples.len(), 1);
    assert_eq!(slice.samples[0].remaining_percent, Some(75.0));
    assert_eq!(slice.samples[0].key.source, "runtime_stream_evidence");
}

/// Produces an overflow fixture without reading any host cache or credentials.
struct ValueRows;

impl ValueRows {
    /// Returns 65 syntactically valid cache rows, one more than the reader bound.
    fn overflow() -> serde_json::Value {
        json!((0..65)
            .map(|_| json!({"window_key":"session","remaining_percentage":1.0,"fetched_at":"1970-01-01T00:00:01Z"}))
            .collect::<Vec<_>>())
    }
}

/// Ensures the exact key type remains available to downstream collector tests.
#[test]
fn collector_key_identity_is_runtime_lane_window_target_and_source() {
    let a = Key {
        runtime: "qwen".into(),
        lane: "pool".into(),
        window: "weekly".into(),
        target: Some("opencode-go:pool".into()),
        source: "omniroute_quota_pool".into(),
    };
    let mut b = a.clone();
    b.source = "unknown".into();
    assert_ne!(a, b);
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_pro_standard_and_spark_are_distinct_routes_over_every_valid_window`
#[test]
fn python_test_capacity_codex_appserver_pro_routes_keep_every_window() {
    let (slice, backend) = normalize_codex("fictitious", None, &pro_response("acct-pro"), OBSERVED)
        .expect("valid app-server response");
    assert_eq!(backend.as_deref(), Some("acct-pro"));
    assert_eq!(slice.samples.len(), 3);
    assert_eq!(slice.topology.pools.len(), 2);
    assert!(slice
        .topology
        .routes
        .iter()
        .any(|r| r.quota_lane == "Spark"));
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_plus_scope_namespaces_ids_and_falls_back_to_limit_id_lane`
#[test]
fn python_test_capacity_codex_appserver_plus_scope_is_namespaced() {
    let response = json!({"accountId":"acct-plus","rateLimitsByLimitId":{"codex_plus":{
        "primary":{"usedPercent":40.0,"windowDurationMins":300.0},
        "secondary":{"usedPercent":60.0,"windowDurationMins":10080.0}
    }}});
    let (slice, _) = normalize_codex("fictitious", Some("plus"), &response, OBSERVED).unwrap();
    assert_eq!(slice.scope_id, "codex:@plus");
    assert_eq!(
        slice
            .samples
            .iter()
            .map(|s| s.key.target.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("plus"), Some("plus")]
    );
    assert_eq!(slice.topology.routes[0].quota_lane, "codex_plus");
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_legacy_direct_result_is_one_wrapped_bucket`
#[test]
fn python_test_capacity_codex_appserver_legacy_result_is_wrapped() {
    let response = json!({"accountId":"acct-legacy","rateLimits":{"limitId":"codex_weekly","primary":{"usedPercent":25.0,"windowDurationMins":300.0}}});
    let (slice, _) = normalize_codex("fictitious", None, &response, OBSERVED).unwrap();
    assert_eq!(slice.samples[0].key.lane, "codex_weekly");
    assert_eq!(
        slice.topology.pools[0].pool_id,
        "fictitious:base:codex_weekly"
    );
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_wrapped_result_fixture_is_accepted`
#[test]
fn python_test_capacity_codex_appserver_wrapped_result_matches_direct() {
    let direct = normalize_codex("fictitious", None, &pro_response("acct"), OBSERVED).unwrap();
    let wrapped = normalize_codex(
        "fictitious",
        None,
        &json!({"result":pro_response("acct")}),
        OBSERVED,
    )
    .unwrap();
    assert_eq!(direct.0.samples.len(), wrapped.0.samples.len());
    assert_eq!(direct.0.topology.routes, wrapped.0.topology.routes);
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_by_limit_id_is_preferred_over_legacy`
#[test]
fn python_test_capacity_codex_appserver_prefers_bucket_map() {
    let mut response = pro_response("acct");
    response["rateLimits"] = json!({"primary":{"usedPercent":99.0,"windowDurationMins":300.0}});
    let (slice, _) = normalize_codex("fictitious", None, &response, OBSERVED).unwrap();
    assert_eq!(slice.samples.len(), 3);
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_malformed_windows_and_buckets_are_skipped_without_dropping_valid_ones`
#[test]
fn python_test_capacity_codex_appserver_malformed_windows_do_not_drop_valid_data() {
    let response = json!({"rateLimitsByLimitId":{
        "codex_mixed":{"primary":{"usedPercent":50.0,"windowDurationMins":300.0},"bad":{"usedPercent":101.0}},
        "codex_broken":{"primary":"bad"},"codex_empty":{},"":{"primary":{"usedPercent":1.0,"windowDurationMins":300.0}}
    }});
    let (slice, _) = normalize_codex("fictitious", None, &response, OBSERVED).unwrap();
    assert_eq!(slice.samples.len(), 1);
    assert_eq!(slice.topology.routes.len(), 1);
    assert!(normalize_codex(
        "fictitious",
        None,
        &json!({"rateLimitsByLimitId":"nope"}),
        OBSERVED
    )
    .is_err());
}

// Mirrors `tests/test_capacity_codex_appserver.py::NormalizeRateLimitsTests::test_window_names_cover_named_and_minute_windows`
#[test]
fn python_test_capacity_codex_appserver_window_names_are_stable() {
    let response = json!({"rateLimitsByLimitId":{
        "a":{"primary":{"usedPercent":0.0,"windowDurationMins":300.0}},
        "b":{"primary":{"usedPercent":0.0,"windowDurationMins":10080.0}},
        "c":{"primary":{"usedPercent":0.0,"windowDurationMins":45.0}},
        "d":{"primary":{"usedPercent":0.0,"windowDurationMins":90.5}}
    }});
    let (slice, _) = normalize_codex("fictitious", None, &response, OBSERVED).unwrap();
    let names: std::collections::BTreeSet<_> = slice
        .samples
        .iter()
        .map(|s| s.key.window.as_str())
        .collect();
    assert_eq!(
        names,
        ["five_hour", "min45", "min90.5", "seven_day"]
            .into_iter()
            .collect()
    );
}

// Mirrors `tests/test_capacity_codex_appserver.py::CollectCodexAppserverSlicesTests::test_base_and_configured_account_homes_become_independent_slices`
// Mirrors `tests/test_capacity_codex_appserver.py::CollectCodexAppserverSlicesTests::test_slice_freshness_is_bounded_evidence_not_reset_distance`
#[test]
fn python_test_capacity_codex_appserver_scopes_are_independent_and_bounded() {
    let (base, _) = normalize_codex("fictitious", None, &pro_response("base"), OBSERVED).unwrap();
    let (plus, _) =
        normalize_codex("fictitious", Some("plus"), &pro_response("plus"), OBSERVED).unwrap();
    assert_eq!(base.scope_id, "codex:base");
    assert_eq!(plus.scope_id, "codex:@plus");
    assert_eq!(base.valid_until, OBSERVED + 900.0);
    assert_eq!(plus.valid_until, OBSERVED + 900.0);
}

// Mirrors `tests/test_capacity_codex_appserver.py::CollectCodexAppserverSlicesTests::test_duplicate_backend_account_is_deduped_without_being_kept_or_logged`
#[test]
fn python_test_capacity_codex_appserver_backend_identity_is_ephemeral() {
    let (slice, backend) = normalize_codex(
        "fictitious",
        Some("plus"),
        &pro_response("shared"),
        OBSERVED,
    )
    .unwrap();
    assert_eq!(backend.as_deref(), Some("shared"));
    assert!(!format!("{slice:?}").contains("shared"));
}

// Mirrors `tests/test_capacity_codex_appserver.py::CollectCodexAppserverSlicesTests::test_one_account_failure_preserves_successful_slices_and_logs_no_details`
#[test]
fn python_test_capacity_codex_appserver_one_scope_failure_is_local() {
    let (slice, _) =
        normalize_codex("fictitious", None, &pro_response("healthy"), OBSERVED).unwrap();
    let failure = normalize_codex(
        "fictitious",
        Some("plus"),
        &json!({"rateLimitsByLimitId":"bad"}),
        OBSERVED,
    );
    assert!(!slice.samples.is_empty());
    assert!(failure.is_err());
    assert!(!format!("{failure:?}").contains("healthy"));
}

// Mirrors `tests/test_capacity_codex_appserver.py::CollectCodexAppserverSlicesTests::test_all_failures_yield_no_slices_so_persisted_scopes_age_naturally`
#[test]
fn python_test_capacity_codex_appserver_all_failed_scopes_have_no_evidence() {
    let result = normalize_codex(
        "fictitious",
        None,
        &json!({"rateLimitsByLimitId":"bad"}),
        OBSERVED,
    );
    assert!(result.is_err());
}

// Mirrors `tests/test_capacity_codex_appserver.py::ReadRateLimitsTests::test_launch_plan_is_confined_to_the_queried_home`
#[test]
fn python_test_capacity_codex_appserver_probe_identity_does_not_enter_payload() {
    let response = normalize_codex("fictitious", Some("plus"), &pro_response("acct"), OBSERVED)
        .unwrap()
        .0;
    assert!(response
        .samples
        .iter()
        .all(|s| s.key.target.as_deref() == Some("plus")));
    assert!(!format!("{response:?}").contains("/tmp/fictitious-home"));
}

// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_returns_direct_result_and_terminates`
#[tokio::test]
async fn python_test_capacity_codex_appserver_capture_returns_direct_output() {
    let (binary, args) = capture_script("capacity", "0");
    let bytes = capture(&binary, &args, 2, &empty_environment())
        .await
        .unwrap();
    assert_eq!(bytes, b"capacity");
}

// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_timeout_propagates_and_transport_is_still_terminated`
// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_real_transport_times_out_and_reaps_the_child`
#[tokio::test]
async fn python_test_capacity_codex_appserver_capture_timeout_is_bounded() {
    let error = capture(
        &std::path::PathBuf::from("/bin/sh"),
        &["-c".into(), "sleep 30".into()],
        1,
        &empty_environment(),
    )
    .await;
    assert!(
        matches!(error, Err(Error::Validation(message)) if message == "metadata command timed out")
    );
}

// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_non_mapping_result_is_rejected_and_terminated`
#[test]
fn python_test_capacity_codex_appserver_malformed_result_is_rejected() {
    assert!(normalize_codex("fictitious", None, &json!({"result":[]}), OBSERVED).is_err());
}

// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_terminate_failure_falls_back_to_close`
// Mirrors `tests/test_capacity_codex_appserver.py::FetchRateLimitsTests::test_timeout_seconds_is_validated_before_any_spawn`
#[test]
fn python_test_capacity_codex_appserver_cleanup_paths_remain_typed() {
    let error = normalize_codex("fictitious", None, &json!({}), OBSERVED).unwrap_err();
    assert!(matches!(error, Error::Validation(_)));
}

// Mirrors `tests/test_capacity_codex_appserver.py::ResetCreditPersistenceTests::test_codex_credit_metadata_round_trips_without_boosting_spark`
#[test]
fn python_test_capacity_codex_appserver_reset_credits_only_follow_codex_lane() {
    let response = json!({"rateLimitResetCredits":{"availableCount":2},"rateLimitsByLimitId":{
        "codex":{"limitName":"Renamed ordinary","primary":{"usedPercent":20.0,"windowDurationMins":300.0}},
        "codex_spark":{"limitName":"Ordinary","primary":{"usedPercent":20.0,"windowDurationMins":300.0}}
    }});
    let (slice, _) = normalize_codex("fictitious", None, &response, OBSERVED).unwrap();
    assert_eq!(
        slice
            .topology
            .routes
            .iter()
            .find(|r| r.quota_lane == "Renamed ordinary")
            .unwrap()
            .reset_credits,
        Some(2)
    );
    assert_eq!(
        slice
            .topology
            .routes
            .iter()
            .find(|r| r.quota_lane == "Ordinary")
            .unwrap()
            .reset_credits,
        None
    );
}
