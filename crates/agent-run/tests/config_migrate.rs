//! The paired `config migrate` / `config rollback` through the real binary,
//! always starting from an untouched schema-16 fixture database (never
//! opened, initialized or registered into before the first assertion).

use fs2::FileExt;
use serde_json::Value;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

/// The untouched Python-era schema-16 database fixture.
fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/baseline/db/current-v16.sqlite")
}

/// Runs the real CLI against `home`, returning (success, JSON or text).
fn run(home: &Path, args: &[&str], fault: bool) -> (bool, Value) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-run"));
    command.arg("--home").arg(home).args(args);
    if fault {
        command.env("AGENT_RUN_MIGRATE_FAULT", "1");
    }
    let output = command.output().unwrap();
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

/// SHA-256 of a file's bytes.
fn sha(path: &Path) -> String {
    agent_run::fs::sha256(&fs::read(path).unwrap())
}

/// The database schema version from its header, without opening it.
fn version(home: &Path) -> u32 {
    let bytes = fs::read(home.join("state.db")).unwrap();
    u32::from_be_bytes([bytes[60], bytes[61], bytes[62], bytes[63]])
}

/// Historical identity of every agent row (columns present in every schema).
fn history(home: &Path) -> Vec<String> {
    let conn = rusqlite::Connection::open_with_flags(
        home.join("state.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let mut statement = conn
        .prepare("SELECT id,status,runtime,model,task,request_json FROM agents ORDER BY id")
        .unwrap();
    let rows = statement
        .query_map([], |row| {
            Ok((0..6)
                .map(|index| format!("{:?}", row.get::<_, rusqlite::types::Value>(index).unwrap()))
                .collect::<Vec<_>>()
                .join("|"))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    rows
}

/// A private disposable home holding a copy of the untouched v16 fixture, a
/// schema-1 config, an explicit mapping (with account declarations) and a
/// stand-in for the installed pre-migration binary.
struct Home {
    _temp: tempfile::TempDir,
    root: PathBuf,
}

impl Home {
    /// Builds the home; nothing opens the database.
    fn new() -> Self {
        let temp = tempfile::Builder::new()
            .prefix("ar-mig-")
            .tempdir_in("/tmp")
            .unwrap();
        let root = temp.path().canonicalize().unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        fs::copy(fixture(), root.join("state.db")).unwrap();
        fs::write(
            root.join("config.toml"),
            format!(
                "schema_version = 1\n[runtimes.codex]\nenabled = true\nadapter = \"codex\"\nbinary = \"/bin/true\"\nhome = \"{0}/codex\"\nmodels = [\"gpt-6-sol\", \"gpt-6-luna\"]\naccounts = [\"personal2\"]\n",
                root.display()
            ),
        )
        .unwrap();
        fs::write(root.join("mapping.toml"), mapping(&root, true)).unwrap();
        fs::write(root.join("bad.toml"), mapping(&root, false)).unwrap();
        let old = root.join("old-agent-run");
        fs::write(&old, "#!/bin/sh\necho 'agent-run 0.12.4'\n").unwrap();
        fs::set_permissions(&old, fs::Permissions::from_mode(0o755)).unwrap();
        Self { _temp: temp, root }
    }

    /// Path text of a file inside the home.
    fn path(&self, name: &str) -> String {
        self.root.join(name).to_string_lossy().into_owned()
    }

    /// Runs `config migrate --apply` with the given mapping.
    fn apply(&self, mapping: &str, fault: bool) -> (bool, Value) {
        let (mapping, old) = (self.path(mapping), self.path("old-agent-run"));
        run(
            &self.root,
            &[
                "config",
                "migrate",
                "--mapping",
                &mapping,
                "--apply",
                "--from-binary",
                &old,
            ],
            fault,
        )
    }

    /// Marks the fixture's running agent terminal, still at schema 16.
    fn finish_agents(&self) {
        rusqlite::Connection::open(self.root.join("state.db"))
            .unwrap()
            .execute("UPDATE agents SET status='succeeded'", [])
            .unwrap();
        assert_eq!(version(&self.root), 16);
    }
}

/// The explicit mapping; `luna = false` leaves a historical model unmapped.
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
[accounts.acct-codex-native]
auth_family = "openai"
reference = "native:codex"
[accounts.acct-codex-personal2]
auth_family = "openai"
reference = "named:codex:personal2"
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
"gpt-6-luna" = ["Default for well-specified implementation, reproducible fixes, bounded review and exploration. Prefer a stronger model when substantial cross-system judgment is required."]
"#,
        home.display(),
        luna
    )
}

/// Dry run, invalid input and every refusal leave the schema-16 database and
/// config byte-identical; ordinary commands refuse instead of upgrading.
#[test]
fn planning_and_refusals_never_touch_the_old_pair() {
    let home = Home::new();
    let (db, config) = (
        sha(&home.root.join("state.db")),
        sha(&home.root.join("config.toml")),
    );
    let unchanged = |label: &str| {
        assert_eq!(sha(&home.root.join("state.db")), db, "{label}: db bytes");
        assert_eq!(version(&home.root), 16, "{label}: schema");
        assert_eq!(
            sha(&home.root.join("config.toml")),
            config,
            "{label}: config"
        );
        assert!(!home.root.join("state.db-wal").exists(), "{label}: wal");
    };
    let (ok, dry) = run(
        &home.root,
        &[
            "config",
            "migrate",
            "--mapping",
            &home.path("mapping.toml"),
            "--dry-run",
        ],
        false,
    );
    assert!(ok, "{dry}");
    assert_eq!(dry["plan"]["state_schema_version"], 16);
    unchanged("dry run");
    let (ok, _) = run(
        &home.root,
        &[
            "config",
            "migrate",
            "--mapping",
            &home.path("bad.toml"),
            "--dry-run",
        ],
        false,
    );
    assert!(!ok);
    unchanged("invalid dry run");
    let (ok, missing) = run(
        &home.root,
        &[
            "config",
            "migrate",
            "--mapping",
            &home.path("mapping.toml"),
            "--apply",
        ],
        false,
    );
    assert!(
        !ok && missing.to_string().contains("--from-binary"),
        "{missing}"
    );
    unchanged("apply without old binary");
    let (ok, running) = home.apply("mapping.toml", false);
    assert!(
        !ok && running.to_string().contains("active agents"),
        "{running}"
    );
    unchanged("active agent");
    let (ok, _) = home.apply("bad.toml", false);
    assert!(!ok);
    unchanged("invalid apply");
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(home.root.join(".api.sock.lock"))
        .unwrap();
    lock.lock_exclusive().unwrap();
    let (ok, live) = home.apply("mapping.toml", false);
    assert!(!ok && live.to_string().contains("broker"), "{live}");
    drop(lock);
    unchanged("live broker");
    for command in [&["accounts", "list"][..], &["init"][..], &["agents"][..]] {
        let (ok, gated) = run(&home.root, command, false);
        assert!(
            !ok && gated.to_string().contains("migration_required"),
            "{command:?}: {gated}"
        );
    }
    unchanged("gated commands");
    assert!(
        !home.root.join("migrations").exists()
            || fs::read_dir(home.root.join("migrations")).unwrap().count() == 0
    );
}

/// config1+DB16 → config2+DB17 with registered accounts; rollback restores
/// config1+DB16 exactly (all rows) and binds the recorded old binary; a
/// post-migration non-agent write makes rollback refuse.
#[test]
fn apply_and_rollback_move_the_whole_pair() {
    let home = Home::new();
    home.finish_agents();
    let rows = history(&home.root);
    let (ok, applied) = home.apply("mapping.toml", false);
    assert!(ok, "{applied}");
    assert_eq!(version(&home.root), 17);
    assert!(fs::read_to_string(home.root.join("config.toml"))
        .unwrap()
        .contains("schema_version = 2"));
    let snapshot = PathBuf::from(applied["snapshot"].as_str().unwrap());
    let manifest: Value =
        serde_json::from_slice(&fs::read(snapshot.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["source_schema_version"], 16);
    assert_eq!(manifest["old_binary"]["version"], "agent-run 0.12.4");
    assert_ne!(
        manifest["old_binary"]["sha256"],
        manifest["target_binary"]["sha256"]
    );
    let (ok, accounts) = run(&home.root, &["accounts", "list"], false);
    assert!(ok, "{accounts}");
    assert_eq!(accounts["accounts"].as_array().unwrap().len(), 2);
    assert_eq!(
        history(&home.root),
        rows,
        "historical rows keep their bytes"
    );
    let (ok, models) = run(&home.root, &["models", "--provider", "codex"], false);
    assert!(
        ok && models.to_string().contains("Default for well-specified"),
        "{models}"
    );
    let frozen: Vec<(String, String)> = [
        "config.v1.toml",
        "state.db",
        "manifest.json",
        "mapping.toml",
        "COMPLETE",
    ]
    .iter()
    .map(|name| (name.to_string(), sha(&snapshot.join(name))))
    .collect();

    // Rollback binds the recorded old binary: a changed executable refuses.
    let old = home.root.join("old-agent-run");
    let original = fs::read(&old).unwrap();
    fs::write(&old, [original.as_slice(), b"# changed\n"].concat()).unwrap();
    let snapshot_text = snapshot.to_string_lossy().into_owned();
    let (ok, changed) = run(
        &home.root,
        &["config", "rollback", "--snapshot", &snapshot_text],
        false,
    );
    assert!(
        !ok && changed.to_string().contains("pre-migration binary"),
        "{changed}"
    );
    fs::write(&old, &original).unwrap();
    assert_eq!(version(&home.root), 17);

    // A post-migration write that creates no agent still blocks rollback.
    let (ok, _) = run(
        &home.root,
        &["accounts", "disable", "acct-codex-personal2"],
        false,
    );
    assert!(ok);
    let (ok, refused) = run(
        &home.root,
        &["config", "rollback", "--snapshot", &snapshot_text],
        false,
    );
    assert!(
        !ok && refused.to_string().contains("state changed"),
        "{refused}"
    );
    assert_eq!(version(&home.root), 17);
    assert!(fs::read_to_string(home.root.join("config.toml"))
        .unwrap()
        .contains("schema_version = 2"));
    for (name, digest) in frozen {
        assert_eq!(
            sha(&snapshot.join(&name)),
            digest,
            "snapshot {name} changed"
        );
    }
}

/// Rollback of an untouched migration restores the pair; a second apply
/// writes a new, distinct snapshot and never touches the first; two
/// concurrent applies cannot both proceed.
#[test]
fn snapshots_are_exclusive_and_immutable() {
    let home = Home::new();
    home.finish_agents();
    let rows = history(&home.root);
    let config_v1 = fs::read(home.root.join("config.toml")).unwrap();
    let (ok, first) = home.apply("mapping.toml", false);
    assert!(ok, "{first}");
    let first = PathBuf::from(first["snapshot"].as_str().unwrap());
    let first_state = sha(&first.join("state.db"));
    let (ok, rolled) = run(
        &home.root,
        &["config", "rollback", "--snapshot", &first.to_string_lossy()],
        false,
    );
    assert!(ok, "{rolled}");
    assert_eq!(rolled["run_binary"]["path"], home.path("old-agent-run"));
    assert_eq!(rolled["state_schema_version"], 16);
    assert_eq!(fs::read(home.root.join("config.toml")).unwrap(), config_v1);
    assert_eq!(version(&home.root), 16);
    assert_eq!(history(&home.root), rows);
    let children: Vec<std::process::Child> = (0..2)
        .map(|_| {
            Command::new(env!("CARGO_BIN_EXE_agent-run"))
                .arg("--home")
                .arg(&home.root)
                .args([
                    "config",
                    "migrate",
                    "--mapping",
                    &home.path("mapping.toml"),
                    "--apply",
                    "--from-binary",
                    &home.path("old-agent-run"),
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();
    let successes = children
        .into_iter()
        .map(|child| child.wait_with_output().unwrap().status.success())
        .filter(|ok| *ok)
        .count();
    assert_eq!(
        successes, 1,
        "exactly one concurrent apply may switch the pair"
    );
    assert_eq!(version(&home.root), 17);
    assert_eq!(
        sha(&first.join("state.db")),
        first_state,
        "first snapshot untouched"
    );
    let complete: Vec<_> = fs::read_dir(home.root.join("migrations"))
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().join("COMPLETE").is_file())
        .collect();
    assert!(complete.len() >= 2, "a new apply writes a new snapshot");
}

/// A failure after the database was upgraded restores the snapshot pair and
/// keeps the snapshot usable.
#[cfg(feature = "test-fixtures")]
#[test]
fn late_publish_failure_restores_the_original_pair() {
    let home = Home::new();
    home.finish_agents();
    let rows = history(&home.root);
    let config = sha(&home.root.join("config.toml"));
    let (ok, failed) = home.apply("mapping.toml", true);
    assert!(!ok && failed.to_string().contains("injected"), "{failed}");
    assert_eq!(version(&home.root), 16);
    assert_eq!(sha(&home.root.join("config.toml")), config);
    assert_eq!(history(&home.root), rows);
    let snapshot = fs::read_dir(home.root.join("migrations"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert!(snapshot.join("COMPLETE").is_file());
    let (ok, retry) = home.apply("mapping.toml", false);
    assert!(ok, "{retry}");
    assert_eq!(version(&home.root), 17);
}
