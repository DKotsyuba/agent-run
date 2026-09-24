//! Retired schema-1 collectors remain inspectable but can never execute provider code.
use agent_run_core::capacity::sources;
use std::{fs, os::unix::fs::PermissionsExt};

/// Old collector names all require explicit external configuration and preserve stored facts.
#[tokio::test]
async fn legacy_collectors_never_launch_and_keep_samples() {
    let home = tempfile::tempdir().unwrap();
    let binary = home.path().join("must-not-run");
    fs::write(&binary, "#!/bin/sh\ntouch invoked\n").unwrap();
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
    let mut text = String::from("schema_version=1\n");
    for (name, adapter, source) in [
        ("a", "codex", "codex_appserver"),
        ("b", "claude", "native"),
        ("c", "glm", "codexbar"),
        ("d", "claude", "none"),
    ] {
        text.push_str(&format!("[runtimes.{name}]\nenabled=true\nadapter={adapter:?}\nbinary={binary:?}\nhome={root:?}\nmodels=[\"fixture\"]\nlimits_source={source:?}\n",binary=binary.to_str().unwrap(),root=home.path().to_str().unwrap()));
    }
    fs::write(home.path().join("config.toml"), text).unwrap();
    let store = agent_run_store::Store::initialize(home.path()).unwrap();
    store.conn.execute("INSERT INTO capacity_samples(runtime,lane,window,source,remaining_percent,observed_at,payload_json) VALUES('a','shared','five_hour','historical',50,1,'null')",[]).unwrap();
    let report = sources::collect(home.path()).await.unwrap();
    assert_eq!(report["ok"], false);
    for row in report["results"].as_array().unwrap() {
        if row["runtime"] == "d" {
            assert_eq!(row["status"], "unsupported");
        } else {
            assert_eq!(row["issues"][0], sources::COLLECTOR_RETIRED);
        }
    }
    assert!(!home.path().join("invoked").exists());
    let count: i64 = store
        .conn
        .query_row("SELECT count(*) FROM capacity_samples", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
}
