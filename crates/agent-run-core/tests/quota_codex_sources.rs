//! Codex app-server quota through the public schema-v2 polling entry:
//! protected-reference identity, alias deduplication, per-model lanes, and
//! truthful round outcomes across rounds. Runs in its own process because
//! the native login is located through the process `CODEX_HOME`.

use agent_run_domain::catalog::{AccountId, AccountRecord, AccountStatus, AuthFamily};
use rusqlite::Connection;
use std::{fs, os::unix::fs::PermissionsExt, path::Path, str::FromStr};
use tempfile::tempdir;

/// Writes a fake app-server that logs which linked auth it saw and answers
/// with a general bucket, a Spark lane, and an unknown lane — or a malformed
/// map while `fail_flag` exists.
fn fake_app_server(root: &Path) -> std::path::PathBuf {
    let path = root.join("fake-codex");
    let log = root.join("probes.log");
    let flag = root.join("fail");
    fs::write(
        &path,
        format!(
            r##"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"method":"initialize"'*)
            printf '%s\n' '{{"id":1,"result":{{}}}}'
            ;;
        *'"method":"account/rateLimits/read"'*)
            cat "$CODEX_HOME/auth.json" >> "{log}"
            echo >> "{log}"
            if [ -e "{flag}" ]; then
                printf '%s\n' '{{"id":2,"result":{{"rateLimitsByLimitId":"broken"}}}}'
            else
                printf '%s\n' '{{"id":2,"result":{{"rateLimitsByLimitId":{{"codex":{{"limitName":"Renamed","primary":{{"usedPercent":25,"windowDurationMins":300,"resetsAt":2000000000}}}},"codex_bengalfox":{{"limitName":"GPT-5.3-Codex-Spark","primary":{{"usedPercent":100,"windowDurationMins":300,"resetsAt":2000000000}}}},"mystery":{{"primary":{{"usedPercent":100,"windowDurationMins":300}}}}}}}}}}'
            fi
            ;;
    esac
done
"##,
            log = log.display(),
            flag = flag.display(),
        ),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// Returns `(lane, window, remaining, payload)` rows stored for `account`.
fn stored(home: &Path, account: &str) -> Vec<(String, String, f64, String)> {
    let conn = Connection::open(home.join("state.db")).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT lane,window,remaining_percent,payload_json FROM capacity_samples \
             WHERE account_id=? ORDER BY lane,window,observed_at",
        )
        .unwrap();
    stmt.query_map([account], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    })
    .unwrap()
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap()
}

/// Native default and named accounts resolve from their protected
/// references (never alias labels), one physical account is probed once
/// across aliases, and each lane governs only its own models; a failing
/// later round reports `ok=false` while earlier samples survive.
#[tokio::test]
async fn codex_accounts_probe_by_reference_with_distinct_lanes() {
    let root = tempdir().unwrap();
    let home = root.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let binary = fake_app_server(root.path());
    // Native login: the host CODEX_HOME, exactly as the provider adapter binds it.
    let native = root.path().join("native-codex");
    fs::create_dir_all(&native).unwrap();
    fs::write(native.join("auth.json"), "native-auth").unwrap();
    std::env::set_var("CODEX_HOME", &native);
    // Named login `work`; the alias labels `work-a`/`work-b` have no homes.
    let work = home.join("accounts/codex/work");
    fs::create_dir_all(&work).unwrap();
    fs::write(work.join("auth.json"), "work-auth").unwrap();
    fs::write(
        home.join("config.toml"),
        format!(
            r#"
schema_version = 2
[harnesses.codex]
binary = "{binary}"
home = "{root}/codex-runtime"
[harnesses.claude-code]
binary = "/bin/true"
home = "{root}/claude-runtime"
[providers.codex-main]
harness = "codex"
connection = {{ kind = "native" }}
auth_family = "openai"
limits_source = "codex_appserver"
[[providers.codex-main.models]]
id = "gpt-6-sol"
[[providers.codex-main.models]]
id = "gpt-5.3-codex-spark"
[[providers.codex-main.bindings]]
label = "main"
account = "acct-native"
[[providers.codex-main.bindings]]
label = "work-a"
account = "acct-work"
models = ["gpt-6-sol"]
[providers.codex-alt]
harness = "codex"
connection = {{ kind = "native" }}
auth_family = "openai"
limits_source = "codex_appserver"
[[providers.codex-alt.models]]
id = "gpt-6-sol"
[[providers.codex-alt.bindings]]
label = "work-b"
account = "acct-work"
"#,
            binary = binary.display(),
            root = root.path().display(),
        ),
    )
    .unwrap();
    let mut store = agent_run_store::Store::initialize(&home).unwrap();
    for (id, reference) in [
        ("acct-native", "native:codex"),
        ("acct-work", "named:codex:work"),
    ] {
        store
            .register_account(&AccountRecord {
                account_id: AccountId::from_str(id).unwrap(),
                auth_family: AuthFamily::from_str("openai").unwrap(),
                secret_ref: reference.parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
    }
    drop(store);

    let report = agent_run_core::capacity::sources::collect(&home)
        .await
        .unwrap();
    assert_eq!(report["ok"], true, "{report}");
    let rows = report["results"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "one row per physical account: {report}");
    let log = fs::read_to_string(root.path().join("probes.log")).unwrap();
    let mut probed: Vec<&str> = log.lines().filter(|line| !line.is_empty()).collect();
    probed.sort_unstable();
    assert_eq!(probed, ["native-auth", "work-auth"]);

    // Native account: the Spark lane governs only Spark, the general lane
    // only the remaining model, and the unknown lane nothing.
    let native_rows = stored(&home, "acct-native");
    assert_eq!(native_rows.len(), 2, "{native_rows:?}");
    let lane = |rows: &[(String, String, f64, String)], name: &str| {
        rows.iter().find(|row| row.0 == name).cloned().unwrap()
    };
    let spark = lane(&native_rows, "codex_bengalfox");
    assert_eq!(spark.2, 0.0);
    assert!(spark.3.contains("gpt-5.3-codex-spark") && !spark.3.contains("gpt-6-sol"));
    let general = lane(&native_rows, "codex");
    assert_eq!(general.2, 75.0);
    assert!(general.3.contains("gpt-6-sol") && !general.3.contains("spark"));
    let native_row = rows
        .iter()
        .find(|row| row["account"] == "acct-native")
        .unwrap();
    assert_eq!(native_row["unmapped_lanes"], 1);
    // Work account binds only gpt-6-sol across both aliases, so the Spark
    // exhaustion never touches it.
    let work_rows = stored(&home, "acct-work");
    assert_eq!(work_rows.len(), 1, "{work_rows:?}");
    assert_eq!(work_rows[0].0, "codex");
    assert_eq!(work_rows[0].2, 75.0);

    // A later failing round is reported truthfully and keeps prior samples.
    fs::write(root.path().join("fail"), "").unwrap();
    let failed = agent_run_core::capacity::sources::collect(&home)
        .await
        .unwrap();
    assert_eq!(failed["ok"], false, "{failed}");
    assert!(failed["results"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["status"] == "failed" && row["issues"][0] == "probe_failed"));
    assert_eq!(stored(&home, "acct-native").len(), 2);
    // The durable ledger now suppresses the next round across processes.
    assert!(home.join("capacity/backoff.json").is_file());
    let suppressed = agent_run_core::capacity::sources::collect(&home)
        .await
        .unwrap();
    assert!(suppressed["results"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["issues"][0] == "backoff"));
}
