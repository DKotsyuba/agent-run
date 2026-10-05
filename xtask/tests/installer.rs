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
        Self::with_tui(false)
    }

    /// Builds a legacy or bundled release from finite executable fixtures.
    fn with_tui(bundled: bool) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let binary = root.join("runtime");
        executable(
            &binary,
            "#!/bin/sh\n[ -z \"$GH_TOKEN\" ] || exit 41\nprintf '%s\\n' \"$AGENT_RUN_HOME\" \"$@\"\n",
        );
        let tui = root.join("observer");
        executable(
            &tui,
            "#!/bin/sh\nprintf 'tui\\n%s\\n' \"$AGENT_RUN_HOME\"\n",
        );
        let candidate = release::build_with_installer(
            &root.join("download"),
            "1.0.0",
            &binary,
            Path::new(env!("CARGO_BIN_EXE_xtask")),
            bundled.then_some(tui.as_path()),
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
        assert!(
            Command::new("tar")
                .args(["--format=ustar", "-czf"])
                .arg(remote.join(asset))
                .arg("-C")
                .arg(&self.candidate)
                .arg(".")
                .status()
                .unwrap()
                .success()
        );
        checksums(&remote, asset);
        fs::write(
            remote.join("latest"),
            "{\"tagName\":\"v1.0.0\",\"isDraft\":false,\"isPrerelease\":false}\n",
        )
        .unwrap();
        remote
    }
}

/// Writes an executable fixture with a finite lifetime.
fn executable(path: &Path, text: &str) {
    fs::write(path, text).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

/// Updates fixture-only external manifest and checksum inventory after archive changes.
/// Synthetic provenance IDs are local fixture data, never a live qualification claim.
fn checksums(remote: &Path, asset: &str) {
    let bytes = fs::read(remote.join(asset)).unwrap();
    let digest = agent_run_platform::fs::sha256(&bytes);
    let artifacts = serde_json::json!([
        {"name":asset,"kind":"bundle","target":"aarch64-apple-darwin","size":bytes.len(),"sha256":digest},
        {"name":"agent-run-1.0.0-source.tar","kind":"source","size":1,"sha256":"b".repeat(64)},
        {"name":"install.sh","kind":"installer","size":1,"sha256":"b".repeat(64)},
        {"name":"acceptance.json","kind":"evidence","size":1,"sha256":"b".repeat(64)}
    ]);
    let manifest = serde_json::json!({"schema_version":1,"product":"agent-run","version":"1.0.0","tag":"v1.0.0","repository":xtask::delivery::REPOSITORY,"repository_id":xtask::delivery::REPOSITORY_ID,"commit":"a".repeat(40),"workflow":{"id":xtask::delivery::WORKFLOW_ID,"path":xtask::delivery::WORKFLOW_PATH,"run_id":17,"run_attempt":1},"standard_version":"1.0.0-rc.2","devkit_version":"0.2.1","baseline":"rust-macos-2026-09-candidate1","trust_profile":"github-attestation","artifacts":artifacts});
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    fs::write(remote.join("release-manifest.json"), &manifest_bytes).unwrap();
    let mut sums = manifest["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|artifact| {
            format!(
                "{}  {}\n",
                artifact["sha256"].as_str().unwrap(),
                artifact["name"].as_str().unwrap()
            )
        })
        .collect::<String>();
    sums.push_str(&format!(
        "{}  release-manifest.json\n",
        agent_run_platform::fs::sha256(&manifest_bytes)
    ));
    fs::write(remote.join("SHA256SUMS"), sums).unwrap();
}

/// Writes two raw USTAR regular members whose paths alias after interior-dot
/// normalization. The tiny fixture is gzipped locally; no payload is executed.
fn malformed_alias_archive(remote: &Path, asset: &str) {
    let mut archive = Vec::new();
    for name in ["bin/agent-run", "bin/./agent-run"] {
        let mut header = [0u8; 512];
        header[..name.len()].copy_from_slice(name.as_bytes());
        header[100..108].copy_from_slice(b"0000644\0");
        header[124..136].copy_from_slice(b"00000000001\0");
        header[148..156].fill(b' ');
        header[156] = b'0';
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        let checksum: u64 = header.iter().map(|byte| *byte as u64).sum();
        header[148..156].copy_from_slice(format!("{checksum:06o}\0 ").as_bytes());
        archive.extend_from_slice(&header);
        archive.push(b'x');
        archive.resize(archive.len() + 511, 0);
    }
    archive.resize(archive.len() + 1024, 0);
    let raw = remote.join("duplicate.tar");
    fs::write(&raw, archive).unwrap();
    assert!(
        Command::new("gzip")
            .arg("-c")
            .stdin(fs::File::open(raw).unwrap())
            .stdout(fs::File::create(remote.join(asset)).unwrap())
            .status()
            .unwrap()
            .success()
    );
    checksums(remote, asset);
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
        None,
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
        "broker", "services", "agent", "writer", "schema", "config", "launcher", "corrupt",
    ] {
        let fixture = Fixture::new();
        let mut connection = None;
        let mut broker = None;
        match reason {
            "broker" | "services" => {
                let file = fs::File::create(fixture.home.join(if reason == "broker" {
                    ".api.sock.lock"
                } else {
                    ".services.lock"
                }))
                .unwrap();
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
    assert!(
        fixture
            .install()
            .unwrap_err()
            .contains("unfinished deployment")
    );
}

/// Exercises both downloaders without network access, including integrity and archive rejection.
#[test]
fn shell_download_and_archive_checks() {
    for downloader in ["curl", "wget"] {
        let fixture = Fixture::with_tui(true);
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
        executable(
            &mocks.join("gh"),
            r#"#!/bin/sh
case "$1/$2" in
 release/view) cat "$TEST_REMOTE/latest" ;;
 api/--hostname)
   case "$4" in
    repos/DKotsyuba/agent-run) printf '{"id":1348534205,"full_name":"DKotsyuba/agent-run"}\n' ;;
    */git/ref/tags/*) printf '{"object":{"type":"tag","sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}\n' ;;
    */git/tags/*) printf '{"sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","object":{"type":"commit","sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}\n' ;;
    *) exit 9 ;;
   esac ;;
 attestation/verify)
   [ "${TEST_ATTEST_FAIL:-0}" = 0 ] || exit 1
   case "$*" in *"--repo DKotsyuba/agent-run"*"--signer-workflow DKotsyuba/agent-run/.github/workflows/release.yml"*"--source-digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"*"--source-ref refs/tags/v1.0.0"*) exit 0 ;; *) exit 9 ;; esac ;;
 *) exit 9 ;;
esac
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
                .env("GH_TOKEN", "SECRET_SENTINEL")
                .env("TMPDIR", fixture.temporary.path());
            command
        };
        let success = run().output().unwrap();
        assert!(
            success.status.success(),
            "{}",
            String::from_utf8_lossy(&success.stderr)
        );
        assert!(fixture.bin.join("agent-run-tui").is_file());
        let refused = run().env("TEST_ATTEST_FAIL", "1").output().unwrap();
        assert!(!refused.status.success());
        assert!(
            String::from_utf8_lossy(&refused.stderr).contains("attestation verification failed")
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
        assert!(
            Command::new("tar")
                .arg("-czf")
                .arg(remote.join(asset))
                .arg("-C")
                .arg(&fixture.candidate)
                .arg(".")
                .status()
                .unwrap()
                .success()
        );
        checksums(&remote, asset);
        let unsafe_archive = run().output().unwrap();
        assert!(!unsafe_archive.status.success());
        assert!(
            String::from_utf8_lossy(&unsafe_archive.stderr).contains("links and special files")
        );
        malformed_alias_archive(&remote, asset);
        let alias = run().output().unwrap();
        assert!(!alias.status.success());
        assert!(
            String::from_utf8_lossy(&alias.stderr).contains("unsafe/duplicate archive member"),
            "{}",
            String::from_utf8_lossy(&alias.stderr)
        );
        assert_eq!(
            fs::read_link(fixture.prefix.join("current")).unwrap(),
            current
        );
        assert!(!fs::read_dir(fixture.temporary.path()).unwrap().any(|e| {
            e.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("agent-run-install.")
        }));
    }
}

/// Both bundled commands use the configured home, honor overrides and reinstall safely.
#[test]
fn bundled_launchers_share_version_and_home() {
    let fixture = Fixture::with_tui(true);
    fixture.install().unwrap();
    fixture.install().unwrap();
    for name in ["agent-run", "agent-run-tui"] {
        for override_home in [None, Some("/tmp/explicit-home")] {
            let mut command = Command::new(fixture.bin.join(name));
            command.env_remove("AGENT_RUN_HOME");
            if let Some(home) = override_home {
                command.env("AGENT_RUN_HOME", home);
            }
            let output = command.output().unwrap();
            assert!(output.status.success(), "{name}");
            let default = fs::canonicalize(&fixture.home).unwrap();
            let expected = override_home.unwrap_or(default.to_str().unwrap());
            let text = String::from_utf8(output.stdout).unwrap();
            assert!(text.lines().any(|line| line == expected), "{name}: {text}");
            if name == "agent-run-tui" {
                assert!(text.starts_with("tui\n"), "wrong executable: {text}");
            }
        }
    }
    release::verify(&fixture.candidate).unwrap();
    fs::write(fixture.candidate.join("bin/agent-run-tui"), "tampered").unwrap();
    assert!(release::verify(&fixture.candidate).is_err());
}

/// A foreign observer command blocks upgrade before any pointer or journal change.
#[test]
fn foreign_tui_blocks_cutover_and_preserves_existing_release() {
    let fixture = Fixture::with_tui(true);
    let legacy = release::build_with_installer(
        &fixture.temporary.path().join("download"),
        "0.9.0",
        &fixture.temporary.path().join("runtime"),
        Path::new(env!("CARGO_BIN_EXE_xtask")),
        None,
    )
    .unwrap();
    installer::install(
        &legacy,
        &fixture.prefix,
        &fixture.home,
        &fixture.bin,
        "0.9.0",
    )
    .unwrap();
    let pointer = fs::read_link(fixture.prefix.join("current")).unwrap();
    let journal = fs::read(fixture.prefix.join("deploy.json")).unwrap();
    let foreign = fixture.bin.join("agent-run-tui");
    fs::write(&foreign, "user-owned observer").unwrap();
    assert!(fixture.install().unwrap_err().contains("unowned launcher"));
    assert_eq!(
        fs::read_link(fixture.prefix.join("current")).unwrap(),
        pointer
    );
    assert_eq!(
        fs::read(fixture.prefix.join("deploy.json")).unwrap(),
        journal
    );
    assert_eq!(fs::read_to_string(foreign).unwrap(), "user-owned observer");
    assert!(!fixture.prefix.join("releases/1.0.0").exists());
}

/// Reusing a sealed legacy version cannot silently omit a newly requested observer.
#[test]
fn adding_tui_to_an_existing_release_requires_a_new_version() {
    let fixture = Fixture::new();
    let error = release::build_with_installer(
        &fixture.temporary.path().join("download"),
        "1.0.0",
        &fixture.temporary.path().join("runtime"),
        Path::new(env!("CARGO_BIN_EXE_xtask")),
        Some(&fixture.temporary.path().join("observer")),
    )
    .unwrap_err();
    assert!(error.contains("predates the bundled TUI"), "{error}");
    release::verify(&fixture.candidate).unwrap();
    assert!(!fixture.candidate.join("bin/agent-run-tui").exists());
}
