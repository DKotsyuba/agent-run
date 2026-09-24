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
        command.env("AGENT_RUN_MIGRATE_FAULT", "after_db");
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

/// Seals `dir` the way `xtask release` does (`bin/agent-run`,
/// `metadata.json`, `SHA256SUMS`, `COMPLETE`). The binary is a script
/// stand-in: this is fixture evidence of release metadata, not of a genuine
/// executable.
fn seal(dir: &Path, schema: u32) {
    fs::create_dir_all(dir.join("bin")).unwrap();
    fs::write(
        dir.join("bin/agent-run"),
        "#!/bin/sh
echo 'agent-run 0.12.4'
",
    )
    .unwrap();
    fs::set_permissions(dir.join("bin/agent-run"), fs::Permissions::from_mode(0o755)).unwrap();
    fs::write(
        dir.join("metadata.json"),
        format!("{{\"version\":\"0.12.4\",\"format\":1,\"schema_version\":{schema}}}\n"),
    )
    .unwrap();
    let sums = ["bin/agent-run", "metadata.json"]
        .iter()
        .map(|name| format!("{}  {name}\n", sha(&dir.join(name))))
        .collect::<String>();
    fs::write(dir.join("SHA256SUMS"), sums).unwrap();
    fs::write(dir.join("COMPLETE"), "complete\n").unwrap();
}

/// A private disposable home holding a copy of the untouched v16 fixture, a
/// schema-1 config, an explicit mapping (with account declarations) and a
/// sealed stand-in for the installed pre-migration release.
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
        seal(&root.join("old-release"), 16);
        Self { _temp: temp, root }
    }

    /// Builds an already-v2/schema-17 home with one existing reference and a retired external Lua binding.
    fn v2() -> Self {
        let home = Self::new();
        fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../tests/fixtures/baseline/db/current-v17.sqlite"),
            home.root.join("state.db"),
        )
        .unwrap();
        let conn = rusqlite::Connection::open(home.root.join("state.db")).unwrap();
        conn.execute_batch("UPDATE agents SET status='succeeded'; INSERT INTO provider_accounts(account_id,auth_family,secret_ref,status,created_at,updated_at) VALUES ('acct-existing','anthropic','env:EXISTING_KEY','enabled',0,0);").unwrap();
        drop(conn);
        let original = format!(
            r#"# Preserve this original v2 configuration byte-for-byte on rollback.
schema_version=2
[harnesses.codex]
binary="/bin/true"
home="{0}/codex"
[harnesses.claude-code]
binary="/bin/true"
home="{0}/claude"
[providers.tenant]
harness="claude-code"
connection={{kind="native"}}
auth_family="anthropic"
limits_source="lua"
collector={{script="/retired/quota.lua"}}
[[providers.tenant.models]]
id="fixture"
[[providers.tenant.bindings]]
label="existing"
account="acct-existing"
"#,
            home.root.display()
        );
        let target=original.replace("limits_source=\"lua\"","limits_source=\"exec\"").replace("collector={script=\"/retired/quota.lua\"}","collector={command=\"/bin/bash\",args=[\"/external/quota.sh\"],source=\"configured-quota\"}");
        fs::write(home.root.join("config.toml"), original).unwrap();
        fs::write(home.root.join("target.toml"), target).unwrap();
        seal(&home.root.join("old-release"), 17);
        home
    }

    /// Path text of a file inside the home.
    fn path(&self, name: &str) -> String {
        self.root.join(name).to_string_lossy().into_owned()
    }

    /// Runs `config migrate --apply` with the given mapping.
    fn apply(&self, mapping: &str, fault: bool) -> (bool, Value) {
        let (mapping, old) = (self.path(mapping), self.path("old-release"));
        run(
            &self.root,
            &[
                "config",
                "migrate",
                "--mapping",
                &mapping,
                "--apply",
                "--from-release",
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

/// An explicit v2 replacement upgrades state without re-registering accounts and rolls back the complete original pair.
#[test]
fn v2_target_config_migration_preserves_accounts_and_history() {
    let home = Home::v2();
    let before_config = fs::read(home.root.join("config.toml")).unwrap();
    let before_db = sha(&home.root.join("state.db"));
    let before_history = history(&home.root);
    let target = home.path("target.toml");
    let old = home.path("old-release");
    let (ok, plan) = run(
        &home.root,
        &["config", "migrate", "--target-config", &target, "--dry-run"],
        false,
    );
    assert!(ok, "{plan}");
    assert_eq!(plan["plan"]["source_config_schema"], 2);
    assert_eq!(sha(&home.root.join("state.db")), before_db);
    assert_eq!(
        fs::read(home.root.join("config.toml")).unwrap(),
        before_config
    );
    assert!(!home.root.join("migrations").exists());
    let (ok, result) = run(
        &home.root,
        &[
            "config",
            "migrate",
            "--target-config",
            &target,
            "--apply",
            "--from-release",
            &old,
        ],
        false,
    );
    assert!(ok, "{result}");
    assert_eq!(version(&home.root), agent_run::state::VERSION as u32);
    assert_eq!(
        fs::read(home.root.join("config.toml")).unwrap(),
        fs::read(&target).unwrap()
    );
    assert_eq!(history(&home.root), before_history);
    let accounts = agent_run::state::Store::open(&home.root)
        .unwrap()
        .list_accounts()
        .unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].account_id.as_str(), "acct-existing");
    assert_eq!(accounts[0].secret_ref.as_str(), "env:EXISTING_KEY");
    let snapshot = result["snapshot"].as_str().unwrap();
    let (ok, result) = run(
        &home.root,
        &["config", "rollback", "--snapshot", snapshot],
        false,
    );
    assert!(ok, "{result}");
    assert_eq!(version(&home.root), 17);
    assert_eq!(
        fs::read(home.root.join("config.toml")).unwrap(),
        before_config
    );
    assert_eq!(history(&home.root), before_history);
}

/// Missing account references and a live service-manager lock refuse before changing the source pair.
#[test]
fn v2_migration_refuses_invalid_target_and_live_manager() {
    let home = Home::v2();
    let source = sha(&home.root.join("state.db"));
    let config = sha(&home.root.join("config.toml"));
    let target = home.path("target.toml");
    let original = fs::read_to_string(&target).unwrap();
    fs::write(&target, original.replace("acct-existing", "acct-missing")).unwrap();
    let (ok, _) = run(
        &home.root,
        &["config", "migrate", "--target-config", &target, "--dry-run"],
        false,
    );
    assert!(!ok);
    fs::write(&target, original).unwrap();
    let lock = fs::File::create(home.root.join(".services.lock")).unwrap();
    lock.lock_exclusive().unwrap();
    let (ok, result) = run(
        &home.root,
        &[
            "config",
            "migrate",
            "--target-config",
            &target,
            "--apply",
            "--from-release",
            &home.path("old-release"),
        ],
        false,
    );
    assert!(!ok, "{result}");
    assert_eq!(sha(&home.root.join("state.db")), source);
    assert_eq!(sha(&home.root.join("config.toml")), config);
    assert!(!home.root.join("migrations").exists());
}

/// The shared publication journal restores a v2 source pair after a failure following the database switch.
#[cfg(feature = "test-fixtures")]
#[test]
fn v2_migration_publish_failure_restores_original_pair() {
    let home = Home::v2();
    let config = fs::read(home.root.join("config.toml")).unwrap();
    let rows = history(&home.root);
    let (ok, _) = run(
        &home.root,
        &[
            "config",
            "migrate",
            "--target-config",
            &home.path("target.toml"),
            "--apply",
            "--from-release",
            &home.path("old-release"),
        ],
        true,
    );
    assert!(!ok);
    assert_eq!(version(&home.root), 17);
    assert_eq!(fs::read(home.root.join("config.toml")).unwrap(), config);
    assert_eq!(history(&home.root), rows);
    assert!(!home.root.join("migrations/in-progress.json").exists());
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
limits_source = "none"
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
        !ok && missing.to_string().contains("--from-release"),
        "{missing}"
    );
    unchanged("apply without old release");
    // An unrelated or incompatible release is refused before any change.
    let incompatible = home.root.join("new-release");
    seal(&incompatible, 17);
    let tampered = home.root.join("tampered-release");
    seal(&tampered, 16);
    fs::write(tampered.join("bin/agent-run"), "#!/bin/sh\n").unwrap();
    for (release, reason) in [
        (&incompatible, "supports schema 17"),
        (&tampered, "not a sealed release"),
        (&home.root.join("config.toml"), "not a sealed release"),
    ] {
        let (ok, refused) = run(
            &home.root,
            &[
                "config",
                "migrate",
                "--mapping",
                &home.path("mapping.toml"),
                "--apply",
                "--from-release",
                &release.to_string_lossy(),
            ],
            false,
        );
        assert!(!ok && refused.to_string().contains(reason), "{refused}");
        unchanged(reason);
    }
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

/// config1+DB16 → config2+current DB with registered accounts; rollback restores
/// config1+DB16 exactly (all rows) and binds the recorded old binary; a
/// post-migration non-agent write makes rollback refuse.
#[test]
fn apply_and_rollback_move_the_whole_pair() {
    let home = Home::new();
    home.finish_agents();
    let rows = history(&home.root);
    let (ok, applied) = home.apply("mapping.toml", false);
    assert!(ok, "{applied}");
    assert_eq!(version(&home.root), agent_run::state::VERSION as u32);
    assert!(fs::read_to_string(home.root.join("config.toml"))
        .unwrap()
        .contains("schema_version = 2"));
    let snapshot = PathBuf::from(applied["snapshot"].as_str().unwrap());
    let manifest: Value =
        serde_json::from_slice(&fs::read(snapshot.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["source_schema_version"], 16);
    assert_eq!(manifest["old_release"]["version"], "0.12.4");
    assert_eq!(manifest["old_release"]["schema_version"], 16);
    assert_eq!(
        manifest["target_binary"]["schema_version"],
        agent_run::state::VERSION
    );
    assert_ne!(
        manifest["old_release"]["binary_sha256"],
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

    // Rollback binds the recorded old release: a changed executable refuses.
    let old = home.root.join("old-release/bin/agent-run");
    let original = fs::read(&old).unwrap();
    fs::write(&old, [original.as_slice(), b"# changed\n"].concat()).unwrap();
    let snapshot_text = snapshot.to_string_lossy().into_owned();
    let (ok, changed) = run(
        &home.root,
        &["config", "rollback", "--snapshot", &snapshot_text],
        false,
    );
    assert!(
        !ok && changed.to_string().contains("pre-migration release"),
        "{changed}"
    );
    fs::write(&old, &original).unwrap();
    assert_eq!(version(&home.root), agent_run::state::VERSION as u32);

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
    assert_eq!(version(&home.root), agent_run::state::VERSION as u32);
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

/// Restoring the old config alone cannot authorize discarding later database
/// writes: an interrupted-looking pair still requires divergence checks.
#[test]
fn old_config_does_not_bypass_rollback_divergence() {
    let home = Home::new();
    home.finish_agents();
    let old_config = fs::read(home.root.join("config.toml")).unwrap();
    let (ok, applied) = home.apply("mapping.toml", false);
    assert!(ok, "{applied}");
    let snapshot = applied["snapshot"].as_str().unwrap();
    let (ok, changed) = run(
        &home.root,
        &["accounts", "disable", "acct-codex-personal2"],
        false,
    );
    assert!(ok, "{changed}");
    fs::write(home.root.join("config.toml"), &old_config).unwrap();
    let before = sha(&home.root.join("state.db"));
    let (ok, result) = run(
        &home.root,
        &["config", "rollback", "--snapshot", snapshot],
        false,
    );
    assert!(!ok, "rollback discarded a later database write: {result}");
    assert_eq!(sha(&home.root.join("state.db")), before);
    assert_eq!(version(&home.root), agent_run::state::VERSION as u32);
    assert_eq!(fs::read(home.root.join("config.toml")).unwrap(), old_config);
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
    assert_eq!(rolled["run_release"]["path"], home.path("old-release"));
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
                    "--from-release",
                    &home.path("old-release"),
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
    assert_eq!(version(&home.root), agent_run::state::VERSION as u32);
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
    let entries: Vec<PathBuf> = fs::read_dir(home.root.join("migrations"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(entries.len(), 1, "only the snapshot remains: {entries:?}");
    assert!(entries[0].join("COMPLETE").is_file());
    let (ok, retry) = home.apply("mapping.toml", false);
    assert!(ok, "{retry}");
    assert_eq!(version(&home.root), agent_run::state::VERSION as u32);
}

/// A resumed-looking pair (old config restored by hand) with an active agent
/// in the migrated database still refuses rollback without writing.
#[test]
fn resumed_rollback_refuses_active_agents() {
    let home = Home::new();
    home.finish_agents();
    let old_config = fs::read(home.root.join("config.toml")).unwrap();
    let (ok, applied) = home.apply("mapping.toml", false);
    assert!(ok, "{applied}");
    let snapshot = applied["snapshot"].as_str().unwrap();
    rusqlite::Connection::open(home.root.join("state.db"))
        .unwrap()
        .execute(
            "UPDATE agents SET status='running' WHERE id=(SELECT min(id) FROM agents)",
            [],
        )
        .unwrap();
    fs::write(home.root.join("config.toml"), &old_config).unwrap();
    let before = sha(&home.root.join("state.db"));
    let (ok, refused) = run(
        &home.root,
        &["config", "rollback", "--snapshot", snapshot],
        false,
    );
    assert!(
        !ok && refused.to_string().contains("active agents"),
        "{refused}"
    );
    assert_eq!(sha(&home.root.join("state.db")), before);
    assert_eq!(version(&home.root), agent_run::state::VERSION as u32);
}

/// A bounded, owned `config migrate --apply` child held at one publication
/// step; killed on drop if a test fails first.
#[cfg(feature = "test-fixtures")]
struct Paused {
    child: std::process::Child,
    control: tempfile::TempDir,
}

#[cfg(feature = "test-fixtures")]
impl Paused {
    /// Starts the apply and waits (at most 30 s) until it reaches `step`.
    fn start(home: &Home, step: &str) -> Self {
        Self::spawn(
            home,
            step,
            &[
                "config",
                "migrate",
                "--mapping",
                &home.path("mapping.toml"),
                "--apply",
                "--from-release",
                &home.path("old-release"),
            ],
        )
    }

    /// Starts `args` and waits (at most 30 s) until it reaches `step`.
    fn spawn(home: &Home, step: &str, args: &[&str]) -> Self {
        let control = tempfile::tempdir_in("/tmp").unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_agent-run"))
            .arg("--home")
            .arg(&home.root)
            .args(args)
            .env(
                "AGENT_RUN_MIGRATE_PAUSE",
                format!("{step}:{}", control.path().display()),
            )
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let paused = Self { child, control };
        for _ in 0..600 {
            if paused.control.path().join("paused").exists() {
                return paused;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        panic!("migration never reached {step}");
    }

    /// Lets the paused process continue and returns whether it succeeded.
    fn finish(&mut self) -> bool {
        fs::write(self.control.path().join("release"), "").unwrap();
        self.child.wait().unwrap().success()
    }
}

#[cfg(feature = "test-fixtures")]
impl Drop for Paused {
    /// Never leaves the child running.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A process killed between the database and config steps (or between the
/// config and the applied record) leaves a journal: ordinary commands refuse
/// the half-published pair, a later row write makes recovery refuse without
/// overwriting it, and a clean state is recovered to config1+DB16.
#[cfg(feature = "test-fixtures")]
#[test]
fn interrupted_apply_is_gated_and_recovered_from_evidence() {
    for step in ["after_db", "after_config"] {
        let home = Home::new();
        home.finish_agents();
        let rows = history(&home.root);
        let config_v1 = fs::read(home.root.join("config.toml")).unwrap();
        let mut paused = Paused::start(&home, step);
        paused.child.kill().unwrap();
        assert!(!paused.child.wait().unwrap().success(), "{step}: killed");
        let journal = home.root.join("migrations/in-progress.json");
        assert!(journal.is_file(), "{step}: journal survives the kill");
        assert_eq!(
            version(&home.root),
            agent_run::state::VERSION as u32,
            "{step}: database was published"
        );
        assert_eq!(
            fs::read(home.root.join("config.toml")).unwrap() == config_v1,
            step == "after_db",
            "{step}: config state"
        );
        for command in [&["accounts", "list"][..], &["agents"][..], &["init"][..]] {
            let (ok, gated) = run(&home.root, command, false);
            assert!(
                !ok && gated.to_string().contains("migration_incomplete"),
                "{step} {command:?}: {gated}"
            );
        }
        let snapshot: Value = serde_json::from_slice(&fs::read(&journal).unwrap()).unwrap();
        let snapshot = snapshot["snapshot"].as_str().unwrap().to_owned();
        let rollback = || {
            run(
                &home.root,
                &["config", "rollback", "--snapshot", &snapshot],
                false,
            )
        };
        if step == "after_config" {
            // A write after the interruption is never discarded.
            let db = rusqlite::Connection::open(home.root.join("state.db")).unwrap();
            db.execute("UPDATE agents SET task=task||'!'", []).unwrap();
            drop(db);
            let before = sha(&home.root.join("state.db"));
            let (ok, refused) = rollback();
            assert!(
                !ok && refused.to_string().contains("state changed"),
                "{refused}"
            );
            assert_eq!(sha(&home.root.join("state.db")), before);
            assert!(journal.is_file(), "refusal keeps the journal");
            let db = rusqlite::Connection::open(home.root.join("state.db")).unwrap();
            db.execute("UPDATE agents SET task=substr(task,1,length(task)-1)", [])
                .unwrap();
        }
        let (ok, recovered) = rollback();
        assert!(ok, "{step}: {recovered}");
        assert!(!journal.exists(), "{step}: journal cleared");
        assert_eq!(fs::read(home.root.join("config.toml")).unwrap(), config_v1);
        assert_eq!(version(&home.root), 16);
        assert_eq!(history(&home.root), rows);
        let (ok, retry) = home.apply("mapping.toml", false);
        assert!(ok, "{step}: retry {retry}");
        assert_eq!(version(&home.root), agent_run::state::VERSION as u32);
    }
}

/// A config edit that lands while the database is already published is kept;
/// only the database this operation owned is restored.
#[cfg(feature = "test-fixtures")]
#[test]
fn external_config_edit_during_publication_is_kept() {
    let home = Home::new();
    home.finish_agents();
    let rows = history(&home.root);
    let mut paused = Paused::start(&home, "after_db");
    let edited = b"schema_version = 1\n# edited by the operator\n";
    fs::write(home.root.join("config.toml"), edited).unwrap();
    fs::write(paused.control.path().join("release"), "").unwrap();
    let mut stderr = String::new();
    std::io::Read::read_to_string(paused.child.stderr.as_mut().unwrap(), &mut stderr).unwrap();
    assert!(!paused.child.wait().unwrap().success());
    assert!(stderr.contains("new content is kept"), "{stderr}");
    assert_eq!(fs::read(home.root.join("config.toml")).unwrap(), edited);
    assert_eq!(version(&home.root), 16);
    assert_eq!(history(&home.root), rows);
    assert!(!home.root.join("migrations/in-progress.json").exists());
}

/// One committed-or-refused write from a fresh connection that never waits:
/// the marker `'w'` is appended to the first agent's task.
fn try_write(home: &Path) -> rusqlite::Result<usize> {
    let conn = rusqlite::Connection::open(home.join("state.db"))?;
    conn.busy_timeout(std::time::Duration::ZERO)?;
    write_with(&conn)
}

/// The marker write through an existing connection.
fn write_with(conn: &rusqlite::Connection) -> rusqlite::Result<usize> {
    conn.execute(
        "UPDATE agents SET task=task||'w' WHERE id=(SELECT min(id) FROM agents)",
        [],
    )
}

/// Whether a write was refused by SQLite locking (not committed).
fn refused_busy(result: rusqlite::Result<usize>) -> bool {
    matches!(
        result,
        Err(rusqlite::Error::SqliteFailure(error, _))
            if matches!(error.code, rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    )
}

/// How many first-agent tasks carry the committed marker write.
fn markers(home: &Path) -> usize {
    history(home)
        .iter()
        .take(1)
        .filter(|row| row.contains("w\")"))
        .count()
}

/// Writers that arrive between the live check and the replacement — for
/// apply and for rollback, in the fixture's WAL mode and in rollback-journal
/// mode — are refused by the database lease rather than overwritten; once
/// the operation completes the same write commits and survives.
#[cfg(feature = "test-fixtures")]
#[test]
fn writers_during_publication_are_refused_not_overwritten() {
    for journal in ["wal", "delete"] {
        let home = Home::new();
        home.finish_agents();
        let preopened = rusqlite::Connection::open(home.root.join("state.db")).unwrap();
        preopened.busy_timeout(std::time::Duration::ZERO).unwrap();
        if journal == "delete" {
            let mode: String = preopened
                .query_row("PRAGMA journal_mode=DELETE", [], |row| row.get(0))
                .unwrap();
            assert_eq!(mode, "delete");
        } else {
            // A WAL handle that is open while the lease is taken refuses it;
            // that case is covered separately below.
            drop(preopened);
        }
        let mut paused = Paused::start(&home, "staged");
        assert!(
            refused_busy(try_write(&home.root)),
            "{journal}: apply fresh writer"
        );
        if journal == "delete" {
            // An idle rollback-journal handle holds no lock when the lease is
            // taken; its later write is refused too.
            let handle = rusqlite::Connection::open(home.root.join("state.db")).unwrap();
            handle.busy_timeout(std::time::Duration::ZERO).unwrap();
            assert!(refused_busy(write_with(&handle)), "{journal}: idle handle");
        }
        assert!(paused.finish(), "{journal}: apply completes");
        assert_eq!(version(&home.root), agent_run::state::VERSION as u32);
        assert_eq!(markers(&home.root), 0, "{journal}: nothing was committed");

        let snapshot = fs::read_dir(home.root.join("migrations"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.join("COMPLETE").is_file())
            .unwrap();
        let snapshot = snapshot.to_string_lossy().into_owned();
        let mut paused = Paused::spawn(
            &home,
            "rollback_checked",
            &["config", "rollback", "--snapshot", &snapshot],
        );
        assert!(
            refused_busy(try_write(&home.root)),
            "{journal}: rollback writer"
        );
        assert!(paused.finish(), "{journal}: rollback completes");
        assert_eq!(version(&home.root), 16);
        assert_eq!(
            try_write(&home.root).unwrap(),
            1,
            "{journal}: writer resumes"
        );
        assert_eq!(markers(&home.root), 1, "{journal}: its write is kept");
    }
}

/// A process that already holds the WAL database open makes apply and
/// rollback refuse before anything is written; after it closes they proceed.
#[test]
fn a_preopened_wal_handle_refuses_the_lease() {
    let home = Home::new();
    home.finish_agents();
    let db = sha(&home.root.join("state.db"));
    let config = sha(&home.root.join("config.toml"));
    let holder = rusqlite::Connection::open(home.root.join("state.db")).unwrap();
    let _: i64 = holder
        .query_row("SELECT count(*) FROM agents", [], |row| row.get(0))
        .unwrap();
    let (ok, refused) = home.apply("mapping.toml", false);
    assert!(!ok && refused.to_string().contains("in use"), "{refused}");
    assert_eq!(sha(&home.root.join("state.db")), db);
    assert_eq!(sha(&home.root.join("config.toml")), config);
    assert!(
        !home.root.join("migrations").exists()
            || fs::read_dir(home.root.join("migrations")).unwrap().count() == 0
    );
    drop(holder);
    let (ok, applied) = home.apply("mapping.toml", false);
    assert!(ok, "{applied}");
    let snapshot = applied["snapshot"].as_str().unwrap();

    let holder = rusqlite::Connection::open(home.root.join("state.db")).unwrap();
    let _: i64 = holder
        .query_row("SELECT count(*) FROM agents", [], |row| row.get(0))
        .unwrap();
    let (ok, refused) = run(
        &home.root,
        &["config", "rollback", "--snapshot", snapshot],
        false,
    );
    assert!(!ok && refused.to_string().contains("in use"), "{refused}");
    assert_eq!(version(&home.root), agent_run::state::VERSION as u32);
    assert!(!home.root.join("migrations/in-progress.json").exists());
    drop(holder);
    let (ok, rolled) = run(
        &home.root,
        &["config", "rollback", "--snapshot", snapshot],
        false,
    );
    assert!(ok, "{rolled}");
    assert_eq!(version(&home.root), 16);
}
