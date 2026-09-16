//! Ported capacity outcome regressions driven through the native app-server probe.

use agent_run_core::capacity::{self, sources};
use agent_run_store::Store;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

/// One Codex runtime configuration and its source/auth fixture files.
struct RuntimeFixture {
    /// Runtime name emitted in the collection report.
    name: String,
    /// Executable passed to the native Codex app-server probe.
    binary: PathBuf,
    /// Configured runtime home used to distinguish missing-home failures.
    runtime_home: PathBuf,
    /// Auth file linked into the probe home for the base account.
    auth: PathBuf,
    /// Labelled account auth contents, used by the fake backend identity.
    accounts: Vec<(String, String)>,
}

/// Builds the executable fake app-server used by all capacity outcome tests.
fn fake_app_server(root: &Path) -> PathBuf {
    let path = root.join("fake-codex-app-server");
    fs::write(
        &path,
        r##"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"method":"initialize"'*)
            printf '%s\n' '{"id":1,"result":{}}'
            ;;
        *'"method":"account/rateLimits/read"'*)
            account=$(cat "$CODEX_HOME/auth.json" 2>/dev/null)
            case "$account" in
                invalid)
                    printf '%s\n' '{"id":2,"result":{"rateLimitsByLimitId":"invalid"}}'
                    ;;
                *)
                    printf '%s\n' '{"id":2,"result":{"accountId":"'"$account"'","rateLimitsByLimitId":{"codex":{"primary":{"usedPercent":25,"windowDurationMins":300,"resetsAt":2000000000}}}}}'
                    ;;
            esac
            ;;
    esac
done
"##,
    )
    .expect("fake app-server writes");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755))
        .expect("fake app-server is executable");
    path
}

/// Creates one runtime fixture and its base/labelled auth documents.
fn runtime_fixture(
    home: &Path,
    name: &str,
    binary: &Path,
    runtime_exists: bool,
    base_account: &str,
    accounts: &[(&str, &str)],
) -> RuntimeFixture {
    let auth = home.join(format!("{name}-auth.json"));
    fs::write(&auth, base_account).expect("base auth writes");
    let runtime_home = home.join(format!("{name}-runtime"));
    if runtime_exists {
        fs::create_dir_all(&runtime_home).expect("runtime home creates");
    }
    let mut labelled = Vec::new();
    for (label, account) in accounts {
        let account_home = home.join("accounts").join("codex").join(label);
        fs::create_dir_all(&account_home).expect("account home creates");
        fs::write(account_home.join("auth.json"), account).expect("account auth writes");
        labelled.push(((*label).into(), (*account).into()));
    }
    RuntimeFixture {
        name: name.into(),
        binary: binary.into(),
        runtime_home,
        auth,
        accounts: labelled,
    }
}

/// Writes a minimal config containing the supplied Codex app-server runtimes.
fn write_config(home: &Path, runtimes: &[RuntimeFixture]) {
    let mut text = String::from("schema_version=1\n[capacity]\nsample_retention=1000\n");
    for runtime in runtimes {
        text.push_str(&format!(
            "[runtimes.{}]\nenabled=true\nadapter=\"codex\"\nbinary={}\nhome={}\nmodels=[\"fixture\"]\nlimits_source=\"codex_appserver\"\nauth={{kind=\"file_link\",source={},target=\"auth.json\"}}\n",
            runtime.name,
            toml::Value::String(runtime.binary.to_string_lossy().into_owned()),
            toml::Value::String(runtime.runtime_home.to_string_lossy().into_owned()),
            toml::Value::String(runtime.auth.to_string_lossy().into_owned()),
        ));
        if !runtime.accounts.is_empty() {
            let labels = runtime
                .accounts
                .iter()
                .map(|(label, _)| format!("\"{label}\""))
                .collect::<Vec<_>>()
                .join(",");
            text.push_str(&format!("accounts=[{labels}]\n"));
        }
    }
    fs::write(home.join("config.toml"), text).expect("capacity config writes");
    Store::initialize(home).expect("capacity store initializes");
}

/// Returns a collection result for one named runtime.
fn result<'a>(report: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    report["results"]
        .as_array()
        .expect("collection reports results")
        .iter()
        .find(|item| item["runtime"] == name)
        .expect("runtime result exists")
}

/// Creates one valid quota slice for atomic persistence tests.
fn slice(runtime: &str, scope: &str, remaining: f64) -> capacity::Slice {
    let key = capacity::Key {
        runtime: runtime.into(),
        lane: "requests".into(),
        window: "five_hour".into(),
        target: None,
        source: "codex_appserver".into(),
    };
    let pool = capacity::Pool {
        pool_id: format!("{runtime}-{scope}"),
        keys: [key.clone()].into_iter().collect(),
    };
    capacity::Slice {
        runtime: runtime.into(),
        scope_id: scope.into(),
        samples: vec![capacity::Sample {
            key: key.clone(),
            remaining_percent: Some(remaining),
            reset_at: Some(2_000.0),
            observed_at: Some(1_000.0),
            valid_until: Some(1_100.0),
        }],
        topology: capacity::Topology {
            pools: vec![pool.clone()],
            routes: vec![capacity::Route {
                route_id: format!("{runtime}-{scope}-route"),
                runtime: runtime.into(),
                account: None,
                quota_lane: "requests".into(),
                pool_ids: vec![pool.pool_id],
                reset_credits: None,
            }],
        },
        observed_at: 1_000.0,
        valid_until: 1_100.0,
    }
}

/// Mirrors `tests/test_capacity_outcomes.py::CapacityOutcomeRegressionTests::test_invalid_slice_does_not_abort_later_valid_slice`
#[tokio::test]
async fn test_invalid_slice_does_not_abort_later_valid_slice() {
    let scratch = tempfile::tempdir().expect("scratch root");
    let binary = fake_app_server(scratch.path());
    let runtimes = vec![
        runtime_fixture(scratch.path(), "bad", &binary, true, "invalid", &[]),
        runtime_fixture(
            scratch.path(),
            "good_one",
            &binary,
            true,
            "backend-one",
            &[],
        ),
        runtime_fixture(
            scratch.path(),
            "good_two",
            &binary,
            true,
            "backend-two",
            &[],
        ),
    ];
    write_config(scratch.path(), &runtimes);

    let report = sources::collect(scratch.path())
        .await
        .expect("round completes");

    assert_eq!(result(&report, "bad")["status"], "failed");
    assert_eq!(result(&report, "good_one")["status"], "collected");
    assert_eq!(result(&report, "good_two")["status"], "collected");
    assert_eq!(
        result(&report, "good_one")["sample_count"].as_u64(),
        Some(1)
    );
    assert_eq!(
        result(&report, "good_two")["sample_count"].as_u64(),
        Some(1)
    );
}

/// Mirrors `tests/test_capacity_outcomes.py::CapacityOutcomeRegressionTests::test_missing_home_and_missing_binary_are_distinct`
#[tokio::test]
async fn test_missing_home_and_missing_binary_are_distinct() {
    let scratch = tempfile::tempdir().expect("scratch root");
    let missing_binary = scratch.path().join("missing-binary");
    let runtimes = vec![
        runtime_fixture(
            scratch.path(),
            "missing_home",
            &missing_binary,
            false,
            "home",
            &[],
        ),
        runtime_fixture(
            scratch.path(),
            "missing_binary",
            &missing_binary,
            true,
            "binary",
            &[],
        ),
    ];
    write_config(scratch.path(), &runtimes);

    let report = sources::collect(scratch.path())
        .await
        .expect("round completes");

    assert_eq!(result(&report, "missing_home")["issues"][0], "home_missing");
    assert_eq!(
        result(&report, "missing_binary")["issues"][0],
        "probe_failed"
    );
}

/// Mirrors `tests/test_capacity_outcomes.py::CapacityOutcomeRegressionTests::test_middle_persistence_failure_keeps_neighboring_scopes_durable`
#[test]
fn test_middle_persistence_failure_keeps_neighboring_scopes_durable() {
    let scratch = tempfile::tempdir().expect("scratch root");
    Store::initialize(scratch.path()).expect("capacity store initializes");
    capacity::persist(scratch.path(), &slice("codex", "middle", 20.0), 1000)
        .expect("seed middle scope");
    let store = Store::open(scratch.path()).expect("capacity store opens");
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER fail_middle BEFORE INSERT ON capacity_route_snapshots
             WHEN NEW.scope_id='middle' BEGIN SELECT RAISE(ABORT, 'provider-secret'); END;",
        )
        .expect("persistence trigger creates");
    drop(store);

    assert_eq!(
        capacity::persist(scratch.path(), &slice("codex", "first", 10.0), 1000).unwrap(),
        1
    );
    assert!(capacity::persist(scratch.path(), &slice("codex", "middle", 25.0), 1000).is_err());
    assert_eq!(
        capacity::persist(scratch.path(), &slice("codex", "last", 30.0), 1000).unwrap(),
        1
    );

    let store = Store::open(scratch.path()).expect("capacity store reopens");
    let mut statement = store
        .conn
        .prepare("SELECT remaining_percent FROM capacity_samples ORDER BY remaining_percent")
        .expect("sample query prepares");
    let rows = statement
        .query_map([], |row| row.get::<_, f64>(0))
        .expect("sample query runs")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("sample rows decode");
    assert_eq!(rows, vec![10.0, 20.0, 30.0]);
}

/// Mirrors `tests/test_capacity_outcomes.py::CapacityOutcomeRegressionTests::test_collect_once_reports_all_failed_codex_and_commits_later_runtime`
#[tokio::test]
async fn test_collect_once_reports_all_failed_codex_and_commits_later_runtime() {
    let scratch = tempfile::tempdir().expect("scratch root");
    let binary = fake_app_server(scratch.path());
    let runtimes = vec![
        runtime_fixture(
            scratch.path(),
            "codex",
            &scratch.path().join("missing-binary"),
            false,
            "codex",
            &[],
        ),
        runtime_fixture(scratch.path(), "healthy", &binary, true, "healthy", &[]),
    ];
    write_config(scratch.path(), &runtimes);

    let report = sources::collect(scratch.path())
        .await
        .expect("round completes");

    assert_eq!(report["ok"], false);
    assert_eq!(result(&report, "codex")["status"], "failed");
    assert_eq!(result(&report, "codex")["sample_count"], 0);
    assert_eq!(result(&report, "healthy")["status"], "collected");
    assert_eq!(result(&report, "healthy")["sample_count"], 1);
    let store = Store::open(scratch.path()).expect("capacity store opens");
    let count: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM capacity_samples", [], |row| {
            row.get(0)
        })
        .expect("sample count reads");
    assert_eq!(count, 1);
}

/// Mirrors `tests/test_capacity_outcomes.py::CapacityOutcomeRegressionTests::test_duplicate_backend_account_id_is_skipped_without_issue`
#[tokio::test]
async fn test_duplicate_backend_account_id_is_skipped_without_issue() {
    let scratch = tempfile::tempdir().expect("scratch root");
    let binary = fake_app_server(scratch.path());
    let runtimes = vec![runtime_fixture(
        scratch.path(),
        "codex",
        &binary,
        true,
        "same-backend",
        &[("one", "same-backend"), ("two", "same-backend")],
    )];
    write_config(scratch.path(), &runtimes);

    let report = sources::collect(scratch.path())
        .await
        .expect("round completes");

    assert_eq!(result(&report, "codex")["status"], "collected");
    assert_eq!(result(&report, "codex")["sample_count"], 1);
    assert_eq!(result(&report, "codex")["issues"], serde_json::json!([]));
}
