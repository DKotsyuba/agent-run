//! Ported bounded-collection-round behaviors of `tests/test_capacity_collect.py`.
//!
//! Python injects a fake adapter loader; Rust has no adapter-loader seam, so a
//! round is driven through the real schema-1 `codex_appserver` source with the
//! runtime binary pointed at a fake app-server. That exercises the same
//! isolation, classification, persistence and retention contracts through the
//! production dispatch path rather than a stub. The failing sibling is a
//! runtime still declaring the retired CodexBar source.

use agent_run_core::capacity::sources;
use std::path::{Path, PathBuf};

/// One runtime entry in a collection-round fixture.
///
/// `name` is the configured runtime name, `adapter` its adapter, `source` the
/// configured `limits_source`, and `enabled` marks a runtime the round must
/// skip entirely. Codex app-server runtimes run the fake app-server binary.
struct Fixture<'a> {
    name: &'a str,
    adapter: &'a str,
    source: &'a str,
    enabled: bool,
}

impl<'a> Fixture<'a> {
    /// Returns one enabled runtime using the given adapter and source.
    fn new(name: &'a str, adapter: &'a str, source: &'a str) -> Self {
        Self {
            name,
            adapter,
            source,
            enabled: true,
        }
    }

    /// Returns a disabled runtime, which a round must never report on.
    fn disabled(name: &'a str) -> Self {
        Self {
            name,
            adapter: "codex",
            source: "codex_appserver",
            enabled: false,
        }
    }
}

/// The recorded weekly bucket: 46 % used, reset at 2026-09-03T16:26:47Z.
const RECORDED: &str = r#"{"rateLimitsByLimitId":{"codex":{"secondary":{"usedPercent":46,"windowDurationMins":10080,"resetsAt":1788452807}}}}"#;

/// Writes a fake Codex app-server answering `account/rateLimits/read` with
/// `result` (a JSON object without single quotes). Returns the path.
fn fake_app_server(root: &Path, result: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = root.join("fake-codex-app-server");
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\nwhile IFS= read -r line; do\n  case \"$line\" in\n    *'\"method\":\"initialize\"'*) printf '%s\\n' '{{\"id\":1,\"result\":{{}}}}' ;;\n    *'\"method\":\"account/rateLimits/read\"'*) printf '%s\\n' '{{\"id\":2,\"result\":{result}}}' ;;\n  esac\ndone\n"
        ),
    )
    .expect("fake app-server writes");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("fake app-server is runnable");
    path
}

/// Builds an agent-run home whose config declares `runtimes` and one retention.
fn round_home(
    runtimes: &[Fixture<'_>],
    app_server: &Path,
    retention: usize,
) -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::tempdir().expect("temporary home");
    let path = temp.path().to_path_buf();
    let auth = path.join("auth.json");
    std::fs::write(&auth, "fixture-account").expect("auth fixture writes");
    let mut text = format!("schema_version=1\n[capacity]\nsample_retention={retention}\n");
    for fixture in runtimes {
        let runtime_home = path.join("runtimes").join(fixture.name);
        std::fs::create_dir_all(&runtime_home).expect("runtime home creates");
        let appserver = fixture.source == "codex_appserver";
        let binary = if appserver {
            app_server.to_path_buf()
        } else {
            PathBuf::from("/bin/true")
        };
        text.push_str(&format!(
            "[runtimes.{}]\nenabled={}\nadapter=\"{}\"\nbinary={}\nhome={}\nmodels=[\"fixture\"]\nlimits_source=\"{}\"\n",
            fixture.name,
            fixture.enabled,
            fixture.adapter,
            toml::Value::String(binary.to_string_lossy().into_owned()),
            toml::Value::String(runtime_home.to_string_lossy().into_owned()),
            fixture.source,
        ));
        if appserver {
            text.push_str(&format!(
                "auth={{kind=\"file_link\",source={},target=\"auth.json\"}}\n",
                toml::Value::String(auth.to_string_lossy().into_owned()),
            ));
        }
    }
    std::fs::write(path.join("config.toml"), text).expect("config writes");
    agent_run_store::Store::initialize(&path).expect("store initializes");
    (temp, path)
}

/// Returns one runtime's result object from a collection round.
fn result_for<'a>(report: &'a serde_json::Value, runtime: &str) -> &'a serde_json::Value {
    report["results"]
        .as_array()
        .expect("every round reports an array of results")
        .iter()
        .find(|item| item["runtime"] == runtime)
        .expect("every enabled runtime reports one result")
}

/// One durable sample row: `(lane, source, remaining, observed, valid_until)`.
///
/// `remaining` and `valid_until` are nullable in the store, so unknown
/// evidence stays distinguishable from a fabricated zero or an absent bound.
type StoredSample = (String, String, Option<f64>, f64, Option<f64>);

/// Returns every persisted sample row, oldest observation first.
fn stored_samples(home: &Path) -> Vec<StoredSample> {
    let store = agent_run_store::Store::open(home).expect("store opens");
    let mut statement = store
        .conn
        .prepare("SELECT lane,source,remaining_percent,observed_at,valid_until FROM capacity_samples ORDER BY observed_at,id")
        .expect("sample query prepares");
    statement
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .expect("sample query runs")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("sample rows decode")
}

/// Mirrors `tests/test_capacity_collect.py::CapacityCollectTests::test_partial_failure_never_blocks_healthy_runtimes`
/// and `test_codexbar_source_stores_samples_with_shelf_life` (now the app-server source).
#[tokio::test]
async fn partial_failure_never_blocks_healthy_runtimes() {
    let scratch = tempfile::tempdir().expect("scratch root");
    let healthy = fake_app_server(scratch.path(), RECORDED);
    let (_temp, home) = round_home(
        &[
            Fixture::new("codex", "codex", "codex_appserver"),
            Fixture::new("failing", "glm", "codexbar"),
            Fixture::new("unsupported", "claude", "none"),
            Fixture::disabled("disabled_rt"),
        ],
        &healthy,
        1_000,
    );

    let report = sources::collect(&home).await.expect("round completes");

    // A disabled runtime is skipped entirely, not reported as unsupported.
    let mut reported: Vec<&str> = report["results"]
        .as_array()
        .expect("results")
        .iter()
        .map(|item| item["runtime"].as_str().expect("runtime name"))
        .collect();
    reported.sort_unstable();
    assert_eq!(reported, vec!["codex", "failing", "unsupported"]);

    assert_eq!(result_for(&report, "codex")["status"], "collected");
    assert_eq!(result_for(&report, "codex")["sample_count"], 1);
    assert_eq!(result_for(&report, "failing")["status"], "failed");
    assert_eq!(
        result_for(&report, "failing")["issues"][0],
        sources::CODEXBAR_RETIRED
    );
    assert_eq!(result_for(&report, "failing")["sample_count"], 0);
    assert_eq!(result_for(&report, "unsupported")["status"], "unsupported");
    assert_eq!(report["ok"], false);

    // One runtime's failure never removes another runtime's evidence, and the
    // sample keeps the source's own bounded shelf life, not the provider
    // reset distance: a weekly reset must not look fresh for days.
    let stored = stored_samples(&home);
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].1, "codex_appserver");
    assert_eq!(stored[0].2, Some(54.0));
    assert_eq!(stored[0].4, Some(stored[0].3 + 900.0));
}

/// Mirrors `tests/test_capacity_collect.py::CapacityCollectTests::test_malformed_samples_and_raising_generators_are_runtime_local`.
#[tokio::test]
async fn malformed_samples_and_raising_generators_are_runtime_local() {
    let scratch = tempfile::tempdir().expect("scratch root");
    // The provider answers successfully with content that is not evidence and
    // carries a sentinel secret in its body.
    let malformed = fake_app_server(
        scratch.path(),
        r#"{"rateLimitsByLimitId":"api_key=must-not-leak"}"#,
    );
    let (_temp, home) = round_home(
        &[Fixture::new("malformed", "codex", "codex_appserver")],
        &malformed,
        1_000,
    );
    let report = sources::collect(&home).await.expect("round completes");

    let result = result_for(&report, "malformed");
    assert_eq!(result["status"], "failed");
    assert_eq!(result["sample_count"], 0);
    // The reason is a fixed code; no provider output reaches the report.
    assert_eq!(result["issues"][0], "probe_failed");
    assert!(
        !report.to_string().contains("must-not-leak"),
        "provider output must never reach the collection report"
    );
}

/// Mirrors `tests/test_capacity_collect.py::CapacityCollectTests::test_no_secrets_or_raw_payload_beyond_structured_sample_fields`.
#[tokio::test]
async fn no_secrets_or_raw_payload_beyond_structured_sample_fields() {
    let scratch = tempfile::tempdir().expect("scratch root");
    let healthy = fake_app_server(scratch.path(), RECORDED);
    let (_temp, home) = round_home(
        &[Fixture::new("codex", "codex", "codex_appserver")],
        &healthy,
        1_000,
    );
    sources::collect(&home).await.expect("round completes");

    let store = agent_run_store::Store::open(&home).expect("store opens");
    let payload: String = store
        .conn
        .query_row("SELECT payload_json FROM capacity_samples", [], |row| {
            row.get(0)
        })
        .expect("one persisted sample");
    // Only the structured sample columns are durable; the provider body is not.
    assert!(!payload.contains("fixture-account"));
    assert!(!payload.contains("auth"));
    assert!(!payload.to_lowercase().contains("token"));
}

/// Mirrors `tests/test_capacity_collect.py::CapacityCollectTests::test_partial_failure_still_prunes_to_global_retention`.
#[tokio::test]
async fn partial_failure_still_prunes_to_global_retention() {
    let scratch = tempfile::tempdir().expect("scratch root");
    let unused = fake_app_server(scratch.path(), RECORDED);
    let (_temp, home) = round_home(&[Fixture::new("broken", "glm", "codexbar")], &unused, 1);

    let store = agent_run_store::Store::open(&home).expect("store opens");
    for observed in [1.0_f64, 2.0_f64] {
        store.conn.execute(
            "INSERT INTO capacity_samples(runtime,lane,window,target,source,remaining_percent,reset_at,observed_at,valid_until,payload_json) VALUES('codex','requests','5h',NULL,'provider',50.0,NULL,?,NULL,'null')",
            [observed],
        ).expect("seed sample inserts");
    }
    drop(store);

    let report = sources::collect(&home).await.expect("round completes");
    assert_eq!(result_for(&report, "broken")["status"], "failed");

    // Retention is global and per round: a round in which every runtime failed
    // still enforces the bound, keeping only the newest observation.
    let stored = stored_samples(&home);
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].3, 2.0);
}
