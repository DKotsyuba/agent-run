//! Disposable installation and offline downloader checks; no production home or long-lived children.

use fs2::FileExt;
use rusqlite::Connection;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};
use tempfile::TempDir;
use xtask::{installer, release};

/// Holds a sealed fixture, isolated state and a launcher directory containing spaces and quotes.
struct Fixture {
    /// Owns all disposable files and removes them when the test ends.
    temporary: TempDir,
    /// Verified release staged as if downloaded from GitHub.
    candidate: PathBuf,
    /// Permanent release storage for this test only.
    prefix: PathBuf,
    /// Isolated configuration and SQLite store.
    home: PathBuf,
    /// Destination of the managed command launcher.
    bin: PathBuf,
}

impl Fixture {
    /// Builds a fake runtime and the real standalone deployment helper into one sealed release.
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let binary = root.join("runtime");
        executable(
            &binary,
            "#!/bin/sh\nprintf '%s\\n' \"$AGENT_RUN_HOME\" \"$@\"\n",
        );
        let candidate = release::build_with_installer(
            &root.join("download"),
            "1.0.0",
            &binary,
            Path::new(env!("CARGO_BIN_EXE_xtask")),
        )
        .unwrap();
        let prefix = root.join("install prefix");
        let home = root.join("user's home");
        let bin = root.join("user bin");
        fs::create_dir(&home).unwrap();
        fs::write(home.join("config.toml"), "schema_version = 2\n").unwrap();
        Self {
            temporary,
            candidate,
            prefix,
            home,
            bin,
        }
    }

    /// Installs the baseline using the same library entry point as the release helper.
    fn install(&self) -> Result<(), String> {
        installer::install(
            &self.candidate,
            &self.prefix,
            &self.home,
            &self.bin,
            "1.0.0",
        )
    }

    /// Creates the minimum current store shape needed by the quiescence and backup checks.
    fn database(&self, schema: u64, status: &str) {
        let connection = Connection::open(self.home.join("state.db")).unwrap();
        connection
            .execute_batch(&format!(
                "PRAGMA user_version={schema}; CREATE TABLE agents (status TEXT);"
            ))
            .unwrap();
        connection
            .execute("INSERT INTO agents VALUES (?)", [status])
            .unwrap();
    }

    /// Archives the sealed candidate and writes the release API/checksum fixtures.
    fn archive(&self) -> PathBuf {
        let remote = self.temporary.path().join("remote");
        fs::create_dir(&remote).unwrap();
        let asset = "agent-run-1.0.0-aarch64-apple-darwin.tar.gz";
        assert!(Command::new("tar")
            .args(["-czf"])
            .arg(remote.join(asset))
            .arg("-C")
            .arg(&self.candidate)
            .arg(".")
            .status()
            .unwrap()
            .success());
        checksums(&remote, asset);
        fs::write(remote.join("latest"), "{\n  \"tag_name\": \"v1.0.0\"\n}\n").unwrap();
        remote
    }
}

/// Writes an executable fixture with a finite lifetime.
fn executable(path: &Path, text: &str) {
    fs::write(path, text).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

/// Writes the exact release checksum using the workspace's existing hash implementation.
fn checksums(remote: &Path, asset: &str) {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(fs::read(remote.join(asset)).unwrap());
    fs::write(remote.join("SHA256SUMS"), format!("{digest:x}  {asset}\n")).unwrap();
}

/// Fresh install, no-op and update keep permanent releases, custom home and state backups.
#[test]
fn install_update_and_noop_preserve_data() {
    let fixture = Fixture::new();
    fixture.database(
        agent_run_platform::release::STORE_SCHEMA_VERSION as u64,
        "succeeded",
    );
    let config = fs::read(fixture.home.join("config.toml")).unwrap();
    fixture.install().unwrap();
    fixture.install().unwrap();
    let next = release::build_with_installer(
        &fixture.temporary.path().join("download"),
        "1.0.1",
        &fixture.temporary.path().join("runtime"),
        Path::new(env!("CARGO_BIN_EXE_xtask")),
    )
    .unwrap();
    installer::install(&next, &fixture.prefix, &fixture.home, &fixture.bin, "1.0.1").unwrap();
    fs::remove_dir_all(fixture.temporary.path().join("download")).unwrap();
    let output = Command::new(fixture.bin.join("agent-run"))
        .env_remove("AGENT_RUN_HOME")
        .arg("literal $argument")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!(
            "{}\nliteral $argument\n",
            fs::canonicalize(&fixture.home).unwrap().display()
        )
    );
    assert_eq!(fs::read(fixture.home.join("config.toml")).unwrap(), config);
    let journal: serde_json::Value =
        serde_json::from_slice(&fs::read(fixture.prefix.join("deploy.json")).unwrap()).unwrap();
    let backup = Path::new(journal["backup"].as_str().unwrap());
    assert_eq!(fs::read(backup.join("config.toml")).unwrap(), config);
    let state = Connection::open(backup.join("state.db")).unwrap();
    assert_eq!(
        state
            .query_row("SELECT status FROM agents", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "succeeded"
    );
    assert_eq!(
        fs::canonicalize(fixture.prefix.join("current")).unwrap(),
        fs::canonicalize(fixture.prefix.join("releases/1.0.1")).unwrap()
    );
}

/// Broker, active work, writer locks, schema/config mismatch and a foreign launcher block cutover.
#[test]
fn preflight_refusals_never_switch() {
    for reason in [
        "broker", "agent", "writer", "schema", "config", "launcher", "corrupt",
    ] {
        let fixture = Fixture::new();
        let mut connection = None;
        let mut broker = None;
        match reason {
            "broker" => {
                let file = fs::File::create(fixture.home.join(".api.sock.lock")).unwrap();
                file.lock_exclusive().unwrap();
                broker = Some(file);
            }
            "agent" => fixture.database(
                agent_run_platform::release::STORE_SCHEMA_VERSION as u64,
                "running",
            ),
            "schema" => fixture.database(
                (agent_run_platform::release::STORE_SCHEMA_VERSION - 1) as u64,
                "succeeded",
            ),
            "writer" => {
                fixture.database(
                    agent_run_platform::release::STORE_SCHEMA_VERSION as u64,
                    "succeeded",
                );
                let writer = Connection::open(fixture.home.join("state.db")).unwrap();
                writer.execute_batch("BEGIN IMMEDIATE").unwrap();
                connection = Some(writer);
            }
            "config" => fs::write(fixture.home.join("config.toml"), "schema_version=1\n").unwrap(),
            "launcher" => {
                fs::create_dir(&fixture.bin).unwrap();
                fs::write(fixture.bin.join("agent-run"), "user's executable").unwrap();
            }
            "corrupt" => fs::write(fixture.candidate.join("bin/agent-run"), "corrupt").unwrap(),
            _ => unreachable!(),
        }
        assert!(fixture.install().is_err(), "{reason}");
        assert!(!fixture.prefix.join("current").exists(), "{reason}");
        assert!(
            !fixture.prefix.join("deploy.json").exists(),
            "preflight must not leave a recovery journal: {reason}"
        );
        drop((connection, broker));
    }
}

/// An already selected version is a safe no-op even while a broker holds the startup lock.
#[test]
fn installed_version_is_not_rewritten_and_pending_recovery_is_preserved() {
    let fixture = Fixture::new();
    fixture.install().unwrap();
    let journal = fs::read(fixture.prefix.join("deploy.json")).unwrap();
    let broker = fs::File::create(fixture.home.join(".api.sock.lock")).unwrap();
    broker.lock_exclusive().unwrap();
    fixture.install().unwrap();
    assert_eq!(
        fs::read(fixture.prefix.join("deploy.json")).unwrap(),
        journal
    );
    fs::write(
        fixture.prefix.join("deploy.json"),
        r#"{"phase":"prepared"}"#,
    )
    .unwrap();
    assert!(fixture
        .install()
        .unwrap_err()
        .contains("unfinished deployment"));
}

/// Exercises both downloaders without network access, including integrity and archive rejection.
#[test]
fn shell_download_and_archive_checks() {
    for downloader in ["curl", "wget"] {
        let fixture = Fixture::new();
        let remote = fixture.archive();
        let mocks = fixture.temporary.path().join("mocks");
        fs::create_dir(&mocks).unwrap();
        executable(
            &mocks.join("uname"),
            "#!/bin/sh\ncase $1 in -s) echo \"${TEST_OS:-Darwin}\" ;; -m) echo arm64 ;; esac\n",
        );
        executable(
            &mocks.join(downloader),
            r#"#!/bin/sh
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output) output=$2; shift ;;
    --output-document=*) output=${1#*=} ;;
    https://*) url=$1 ;;
  esac
  shift
done
cp "$TEST_REMOTE/${url##*/}" "$output"
"#,
        );
        let run = || {
            let mut command = Command::new("sh");
            command
                .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../install.sh"))
                .args(["--downloader", downloader, "--home"])
                .arg(&fixture.home)
                .arg("--prefix")
                .arg(&fixture.prefix)
                .arg("--bin-dir")
                .arg(&fixture.bin)
                .env(
                    "PATH",
                    format!("{}:{}", mocks.display(), std::env::var("PATH").unwrap()),
                )
                .env("TEST_REMOTE", &remote)
                .env("TMPDIR", fixture.temporary.path());
            command
        };
        let success = run().output().unwrap();
        assert!(
            success.status.success(),
            "{}",
            String::from_utf8_lossy(&success.stderr)
        );
        let current = fs::read_link(fixture.prefix.join("current")).unwrap();
        fs::write(
            remote.join("SHA256SUMS"),
            format!(
                "{}  agent-run-1.0.0-aarch64-apple-darwin.tar.gz\n",
                "0".repeat(64)
            ),
        )
        .unwrap();
        let bad = run().output().unwrap();
        assert!(!bad.status.success());
        assert!(String::from_utf8_lossy(&bad.stderr).contains("checksum mismatch"));
        let unsupported = run().env("TEST_OS", "Linux").output().unwrap();
        assert!(!unsupported.status.success());
        assert!(String::from_utf8_lossy(&unsupported.stderr).contains("only macOS"));
        std::os::unix::fs::symlink("/tmp", fixture.candidate.join("unsafe-link")).unwrap();
        let asset = "agent-run-1.0.0-aarch64-apple-darwin.tar.gz";
        assert!(Command::new("tar")
            .arg("-czf")
            .arg(remote.join(asset))
            .arg("-C")
            .arg(&fixture.candidate)
            .arg(".")
            .status()
            .unwrap()
            .success());
        checksums(&remote, asset);
        let unsafe_archive = run().output().unwrap();
        assert!(!unsafe_archive.status.success());
        assert!(String::from_utf8_lossy(&unsafe_archive.stderr).contains("links and special files"));
        assert_eq!(
            fs::read_link(fixture.prefix.join("current")).unwrap(),
            current
        );
        assert!(!fs::read_dir(fixture.temporary.path()).unwrap().any(|e| e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("agent-run-install.")));
    }
}
