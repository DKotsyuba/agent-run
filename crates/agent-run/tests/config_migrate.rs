//! One-time `config migrate` / `config rollback` through the real binary on
//! a disposable home seeded with a historical (Python-written) database.

use serde_json::Value;
use std::{fs, path::Path, process::Command};

/// Runs the real CLI against `home`, returning (success, JSON or error text).
fn run(home: &Path, args: &[&str]) -> (bool, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .arg("--home")
        .arg(home)
        .args(args)
        .output()
        .unwrap();
    let text = if output.status.success() {
        output.stdout
    } else {
        [output.stdout, output.stderr].concat()
    };
    (
        output.status.success(),
        serde_json::from_slice(&text)
            .unwrap_or(Value::String(String::from_utf8_lossy(&text).into())),
    )
}

/// Digest of every historical agent, message and event row.
fn history(home: &Path) -> Vec<String> {
    let conn = rusqlite::Connection::open(home.join("state.db")).unwrap();
    let mut rows = Vec::new();
    for table in ["agents", "messages", "events"] {
        let mut statement = conn
            .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
            .unwrap();
        let columns = statement.column_count();
        let mut query = statement.query([]).unwrap();
        while let Some(row) = query.next().unwrap() {
            let values: Vec<String> = (0..columns)
                .map(|index| format!("{:?}", row.get::<_, rusqlite::types::Value>(index).unwrap()))
                .collect();
            rows.push(format!("{table}:{}", values.join("|")));
        }
    }
    rows
}

/// The schema-1 config of the fixture home.
fn v1_config(home: &Path) -> String {
    format!(
        "schema_version = 1\n[runtimes.codex]\nenabled = true\nadapter = \"codex\"\nbinary = \"/bin/true\"\nhome = \"{0}/codex\"\nmodels = [\"gpt-6-sol\", \"gpt-6-luna\"]\naccounts = [\"personal2\"]\n",
        home.display()
    )
}

/// The explicit operator mapping, with recommendation prose from the seed.
fn mapping(home: &Path, luna: bool) -> String {
    let luna = if luna {
        "\"gpt-6-luna\" = \"gpt-6-luna\"\n"
    } else {
        ""
    };
    format!(
        r#"[harnesses.codex]
binary = "/bin/true"
home = "{0}/codex"
[harnesses.claude-code]
binary = "/bin/true"
home = "{0}/claude"
[runtimes.codex]
provider = "codex"
harness = "codex"
connection = {{ kind = "native" }}
auth_family = "openai"
limits_source = "codex_appserver"
global_account = "acct-codex-native"
labelled_accounts = {{ personal2 = "acct-codex-personal2" }}
[runtimes.codex.native_models]
"gpt-6-sol" = "gpt-6-sol"
{1}[runtimes.codex.model_recommendations]
"gpt-6-sol" = ["Use for connected implementation, cross-system diagnosis, security or concurrency reasoning, and substantive reviews with interacting constraints."]
"gpt-6-luna" = ["Default for well-specified implementation, reproducible fixes, bounded review and exploration. Prefer a stronger model when substantial cross-system judgment is required."]
"#,
        home.display(),
        luna
    )
}

/// Dry run writes nothing; apply refuses a live agent, then snapshots and
/// switches; models reads the migrated prose; history bytes are unchanged;
/// a bad mapping or diverged config fails closed; rollback restores v1.
#[test]
fn migrate_apply_and_rollback_preserve_history() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/baseline/db/historical-v15.sqlite"),
        home.join("state.db"),
    )
    .unwrap();
    let v1 = v1_config(&home);
    fs::write(home.join("config.toml"), &v1).unwrap();
    for (id, reference) in [
        ("acct-codex-native", "native:codex"),
        ("acct-codex-personal2", "named:codex:personal2"),
    ] {
        let (ok, value) = run(
            &home,
            &[
                "accounts",
                "register",
                "--id",
                id,
                "--auth-family",
                "openai",
                "--reference",
                reference,
            ],
        );
        assert!(ok, "{value}");
    }
    fs::write(home.join("mapping.toml"), mapping(&home, true)).unwrap();
    fs::write(home.join("bad.toml"), mapping(&home, false)).unwrap();
    let good = home.join("mapping.toml");
    let good = good.to_str().unwrap();
    let broken = home.join("bad.toml");
    let broken = broken.to_str().unwrap();
    let before = history(&home);

    let (ok, dry) = run(
        &home,
        &["config", "migrate", "--mapping", good, "--dry-run"],
    );
    assert!(ok, "{dry}");
    assert_eq!(dry["applied"], false);
    assert!(dry["plan"]["config_toml"]
        .as_str()
        .unwrap()
        .contains("Default for well-specified implementation"));
    assert_eq!(fs::read_to_string(home.join("config.toml")).unwrap(), v1);

    // The fixture still has a running agent: apply must refuse, untouched.
    let (ok, refused) = run(&home, &["config", "migrate", "--mapping", good, "--apply"]);
    assert!(!ok, "{refused}");
    assert!(refused.to_string().contains("active agents"));
    assert_eq!(fs::read_to_string(home.join("config.toml")).unwrap(), v1);
    let (ok, bad) = run(
        &home,
        &["config", "migrate", "--mapping", broken, "--apply"],
    );
    assert!(!ok, "{bad}");
    assert_eq!(fs::read_to_string(home.join("config.toml")).unwrap(), v1);

    // Make the fixture run terminal and resumable-looking (a Rust-valid id
    // and a native session) so the schema-2 refusal is the only gate left.
    rusqlite::Connection::open(home.join("state.db"))
        .unwrap()
        .execute_batch(
            r#"PRAGMA foreign_keys=OFF;
             UPDATE agents SET status='succeeded',runtime_session_id='native-session-1',
               id='ag-20260101-000000-0000000001',root_agent_id='ag-20260101-000000-0000000001',
               request_json='{"runtime":"codex","model":"gpt-6-sol","profile":"review","task":"t","workdir":"/tmp"}';
             UPDATE events SET agent_id='ag-20260101-000000-0000000001';"#,
        )
        .unwrap();
    // Terminal now; this is the history the migration must not touch.
    let before = history(&home);
    let (ok, applied) = run(&home, &["config", "migrate", "--mapping", good, "--apply"]);
    assert!(ok, "{applied}");
    let snapshot = applied["snapshot"].as_str().unwrap().to_owned();
    let snapshot = Path::new(&snapshot);
    assert!(snapshot.join("COMPLETE").is_file());
    let manifest: Value =
        serde_json::from_slice(&fs::read(snapshot.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["binary"]["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(manifest["agents"], 1);
    assert_eq!(
        fs::read_to_string(snapshot.join("config.v1.toml")).unwrap(),
        v1
    );
    assert!(fs::read_to_string(home.join("config.toml"))
        .unwrap()
        .contains("schema_version = 2"));
    let (ok, models) = run(&home, &["models", "--provider", "codex"]);
    assert!(ok, "{models}");
    let text = models.to_string();
    assert!(text.contains("Use for connected implementation"), "{text}");
    assert_eq!(history(&home), before, "migration never rewrites history");
    // A schema-1 run is never remapped or replayed under schema 2.
    let id: String = rusqlite::Connection::open(home.join("state.db"))
        .unwrap()
        .query_row("SELECT id FROM agents", [], |row| row.get(0))
        .unwrap();
    let refused = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(agent_run::service::Service::new(home.clone()).resume(
            &id.parse().unwrap(),
            "continue".into(),
            None,
            None,
            None,
        ))
        .unwrap_err();
    assert_eq!(
        refused.machine_code().as_str(),
        "Unsupported",
        "{refused:?}"
    );
    assert!(refused
        .to_string()
        .contains("legacy_continuation_unavailable"));
    assert_eq!(history(&home), before, "a refused resume writes nothing");

    // A post-migration config edit is divergence: rollback refuses.
    let migrated = fs::read(home.join("config.toml")).unwrap();
    fs::write(
        home.join("config.toml"),
        [migrated.as_slice(), b"\n"].concat(),
    )
    .unwrap();
    let snapshot_arg = snapshot.to_str().unwrap();
    let (ok, diverged) = run(&home, &["config", "rollback", "--snapshot", snapshot_arg]);
    assert!(!ok, "{diverged}");
    fs::write(home.join("config.toml"), &migrated).unwrap();
    let (ok, rolled) = run(&home, &["config", "rollback", "--snapshot", snapshot_arg]);
    assert!(ok, "{rolled}");
    assert_eq!(fs::read_to_string(home.join("config.toml")).unwrap(), v1);
    assert_eq!(history(&home), before);
}
