//! Offline subprocess fixtures exercise real observer/publisher Rust control flow.
//! Synthetic GitHub/source identities never claim live publication or host acceptance.
use serde_json::{Value, json};
use std::{
    fs,
    io::Read,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};
use xtask::delivery::{
    Acceptance, Artifact, Check, Identity, Manifest, REPOSITORY, REPOSITORY_ID, WORKFLOW_ID,
    WORKFLOW_PATH, Workflow,
};

/// Owns only disposable source, fake GitHub metadata/assets and finite tools.
struct Fixture {
    /// Private fixture lifetime; no installed user home is touched.
    temp: tempfile::TempDir,
    /// Repository-shaped clean source served through fake gh/git.
    root: PathBuf,
    /// Exact named release inventory.
    assets: PathBuf,
    /// PATH tools intercept all network/ref operations locally.
    tools: PathBuf,
    /// Accepted synthetic full SHA; no actual Git refs are mutated.
    manifest: Manifest,
}
/// Writes one finite shell executable in an isolated fixture.
fn executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}
/// Stores fixture JSON with no remote writes or interpreter subprocess.
fn record(path: &Path, value: &impl serde::Serialize) {
    fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
}

impl Fixture {
    /// Constructs hashes from real fixture archive bytes and synthetic successful
    /// evidence. The fake run/repository IDs model contract boundaries only.
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("source");
        let assets = temp.path().join("assets");
        let tools = temp.path().join("tools");
        for path in [&root, &assets, &tools] {
            fs::create_dir(path).unwrap();
        }
        let version = env!("CARGO_PKG_VERSION");
        fs::write(
            root.join("Cargo.toml"),
            format!("[workspace.package]\nversion = \"{version}\"\n"),
        )
        .unwrap();
        fs::write(root.join("family.toml"),"standard_version = \"1.0.0-rc.2\"\ndevkit_version = \"0.2.1\"\nbaseline = \"fixture-baseline\"\n[release]\nenabled = true\ntrust_profile = \"github-attestation\"\n[compatibility]\nqualified_targets = [\"aarch64-apple-darwin\"]\n").unwrap();
        fs::write(
            root.join("rust-toolchain.toml"),
            "[toolchain]\nchannel = \"1.98.1\"\n",
        )
        .unwrap();
        fs::write(
            root.join("CHANGELOG.md"),
            format!("# Changes\n## {version}\nFixture notes.\n"),
        )
        .unwrap();
        let binary = temp.path().join("binary");
        executable(&binary, "#!/bin/sh\nexit 0\n");
        let bundle = xtask::release::build(&temp.path().join("sealed"), version, &binary).unwrap();
        let bundle_name = format!("agent-run-{version}-aarch64-apple-darwin.tar.gz");
        assert!(
            Command::new("tar")
                .args(["--format=ustar", "-czf"])
                .arg(assets.join(&bundle_name))
                .arg("-C")
                .arg(&bundle)
                .arg(".")
                .status()
                .unwrap()
                .success()
        );
        let source_name = format!("agent-run-{version}-source.tar");
        assert!(
            Command::new("tar")
                .args(["--format=ustar", "-cf"])
                .arg(assets.join(&source_name))
                .arg("-C")
                .arg(&root)
                .arg("Cargo.toml")
                .status()
                .unwrap()
                .success()
        );
        fs::write(assets.join("install.sh"), "#!/bin/sh\nexit 0\n").unwrap();
        let workflow = Workflow {
            id: WORKFLOW_ID,
            path: WORKFLOW_PATH.into(),
            run_id: 17,
            run_attempt: 1,
        };
        let identity = Identity {
            product: "agent-run".into(),
            version: version.into(),
            tag: format!("v{version}"),
            repository: REPOSITORY.into(),
            repository_id: REPOSITORY_ID,
            commit: "a".repeat(40),
            workflow: Some(workflow.clone()),
            target: "aarch64-apple-darwin".into(),
            toolchain: "1.98.1".into(),
            standard_version: "1.0.0-rc.2".into(),
            devkit_version: "0.2.1".into(),
            baseline: "fixture-baseline".into(),
        };
        let bundle_artifact = Artifact {
            name: bundle_name.clone(),
            kind: "bundle".into(),
            target: Some(identity.target.clone()),
            size: fs::metadata(assets.join(&bundle_name)).unwrap().len(),
            sha256: agent_run_platform::fs::sha256(&fs::read(assets.join(&bundle_name)).unwrap()),
        };
        let checks = [
            "workspace",
            "native-build",
            "source-archive",
            "integration",
            "dependency-policy",
            "exact-payload",
        ]
        .into_iter()
        .map(|id| Check {
            check_id: id.into(),
            argv_sha256: "b".repeat(64),
            stdout_sha256: "b".repeat(64),
            stderr_sha256: "b".repeat(64),
            exit_code: 0,
        })
        .collect();
        record(
            &assets.join("acceptance.json"),
            &Acceptance {
                schema_version: 1,
                identity: identity.clone(),
                scope: "github-actions".into(),
                checks,
                qualified_hosts: vec![],
                payload: Some(bundle_artifact),
            },
        );
        let artifacts = [
            (bundle_name, "bundle"),
            (source_name, "source"),
            ("install.sh".into(), "installer"),
            ("acceptance.json".into(), "evidence"),
        ]
        .into_iter()
        .map(|(name, kind)| Artifact {
            size: fs::metadata(assets.join(&name)).unwrap().len(),
            sha256: agent_run_platform::fs::sha256(&fs::read(assets.join(&name)).unwrap()),
            name,
            kind: kind.into(),
            target: (kind == "bundle").then(|| identity.target.clone()),
        })
        .collect();
        let manifest = Manifest {
            schema_version: 1,
            product: identity.product,
            version: identity.version,
            tag: identity.tag,
            repository: identity.repository,
            repository_id: identity.repository_id,
            commit: identity.commit,
            workflow,
            standard_version: identity.standard_version,
            devkit_version: identity.devkit_version,
            baseline: identity.baseline,
            trust_profile: "github-attestation".into(),
            artifacts,
        };
        record(&assets.join("release-manifest.json"), &manifest);
        let mut sums = manifest
            .artifacts
            .iter()
            .map(|a| format!("{}  {}\n", a.sha256, a.name))
            .collect::<String>();
        sums.push_str(&format!(
            "{}  release-manifest.json\n",
            agent_run_platform::fs::sha256(
                &fs::read(assets.join("release-manifest.json")).unwrap()
            )
        ));
        fs::write(assets.join("SHA256SUMS"), sums).unwrap();
        record(
            &temp.path().join("repo.json"),
            &json!({"id":REPOSITORY_ID,"full_name":REPOSITORY}),
        );
        record(
            &temp.path().join("workflow.json"),
            &json!({"id":WORKFLOW_ID,"path":WORKFLOW_PATH,"state":"active"}),
        );
        record(
            &temp.path().join("tag.json"),
            &json!({"ref":format!("refs/tags/{}",manifest.tag),"object":{"type":"tag","sha":"b".repeat(40)}}),
        );
        record(
            &temp.path().join("tag-object.json"),
            &json!({"sha":"b".repeat(40),"object":{"type":"commit","sha":manifest.commit}}),
        );
        let run = json!({"id":17,"run_attempt":1,"workflow_id":WORKFLOW_ID,"path":WORKFLOW_PATH,"head_sha":manifest.commit,"head_branch":manifest.tag,"event":"push","repository":{"id":REPOSITORY_ID,"full_name":REPOSITORY},"status":"completed","conclusion":"success"});
        record(&temp.path().join("run.json"), &run);
        record(
            &temp.path().join("runs.json"),
            &json!({"workflow_runs":[run]}),
        );
        let release_assets=fs::read_dir(&assets).unwrap().map(|entry|{let entry=entry.unwrap();json!({"name":entry.file_name().to_str().unwrap(),"size":entry.metadata().unwrap().len(),"state":"uploaded"})}).collect::<Vec<_>>();
        let published = json!({"id":29,"tag_name":manifest.tag,"draft":false,"prerelease":false,"published_at":"fixture","assets":release_assets});
        record(&temp.path().join("published.json"), &published);
        let mut draft = published;
        draft["draft"] = json!(true);
        draft["published_at"] = Value::Null;
        record(&temp.path().join("draft.json"), &draft);
        executable(
            &tools.join("git"),
            r#"#!/bin/sh
case "$1" in
 status) exit 0 ;;
 cat-file) echo tag ;;
 rev-parse) echo aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ;;
 merge-base) exit 0 ;;
 *) exit 9 ;;
esac
"#,
        );
        executable(
            &tools.join("gh"),
            r#"#!/bin/sh
case "$1" in
 api)
  include=0
  for arg do case "$arg" in repos/*) endpoint=$arg ;; --include) include=1 ;; esac; done
  if [ "$include" = 1 ]; then
    if [ "$endpoint" = repos/DKotsyuba/agent-run ] && [ "${TEST_API_RETRY:-0}" = 1 ]; then
      count=0; [ ! -f "$TEST_FIXTURE/retry-count" ] || read count < "$TEST_FIXTURE/retry-count"
      count=$((count+1)); printf '%s\n' "$count" > "$TEST_FIXTURE/retry-count"
      case "$count" in
        1) printf 'HTTP/2.0 503 Service Unavailable\r\n\r\n{"error":"UPSTREAM_CANARY"}'; exit 1 ;;
        2) printf 'HTTP/2.0 429 Too Many Requests\r\nRetry-After: 1\r\n\r\n{"error":"UPSTREAM_CANARY"}'; exit 1 ;;
      esac
    fi
    case "$endpoint" in
      */releases\?*) if [ "${TEST_RELEASE_LIST_DENIED:-0}" = 1 ]; then printf 'HTTP/2.0 403 Forbidden\r\n\r\n{}'; exit 1; fi ;;
      */releases/tags/*) if [ ! -f "$TEST_FIXTURE/release.json" ] || grep -q '"draft":true' "$TEST_FIXTURE/release.json"; then printf 'HTTP/2.0 404 Not Found\r\n\r\n{}'; exit 1; fi ;;
    esac
    printf 'HTTP/2.0 200 OK\r\nContent-Type: application/json\r\n\r\n'
  fi
  case "$endpoint" in
   repos/DKotsyuba/agent-run) cat "$TEST_FIXTURE/repo.json" ;;
   */actions/workflows/346709607) cat "$TEST_FIXTURE/workflow.json" ;;
   */actions/workflows/*/runs*) cat "$TEST_FIXTURE/runs.json" ;;
   */actions/runs/17) cat "$TEST_FIXTURE/run.json" ;;
   */git/ref/tags/*) cat "$TEST_FIXTURE/tag.json" ;;
   */git/tags/*) cat "$TEST_FIXTURE/tag-object.json" ;;
   */releases/tags/*) if [ -f "$TEST_FIXTURE/release.json" ]; then cat "$TEST_FIXTURE/release.json"; else echo 'HTTP 404' >&2; exit 1; fi ;;
   */releases\?*) if [ -f "$TEST_FIXTURE/releases.json" ]; then cat "$TEST_FIXTURE/releases.json"; elif [ -f "$TEST_FIXTURE/release.json" ]; then printf '[';cat "$TEST_FIXTURE/release.json";printf ']';else printf '[]';fi ;;
   */releases/29) if [ -f "$TEST_FIXTURE/release-id.json" ]; then cat "$TEST_FIXTURE/release-id.json";else cat "$TEST_FIXTURE/release.json";fi ;;
   */contents/*) if [ "${TEST_STALL_STAGE:-}" = source ]; then touch "$TEST_FIXTURE/leaf-started"; sleep 30; exit 1; fi; name=${endpoint##*/};name=${name%%\?*};cat "$TEST_FIXTURE/source/$name" ;;
   *) exit 9 ;;
  esac ;;
 release)
  operation=$2; shift 2
  case "$operation" in
   download)
    if [ "${TEST_STALL_STAGE:-}" = download ]; then touch "$TEST_FIXTURE/leaf-started"; sleep 30; exit 1; fi
    patterns=
    while [ "$#" -gt 0 ]; do case "$1" in --dir) directory=$2;shift ;; --pattern) patterns="$patterns $2";shift ;; esac;shift;done
    for name in $patterns; do [ ! -e "$directory/$name" ] || exit 9;cp "$TEST_FIXTURE/assets/$name" "$directory/$name";done ;;
   create) echo create >> "$TEST_FIXTURE/operations";cp "$TEST_FIXTURE/draft.json" "$TEST_FIXTURE/release.json" ;;
   edit) echo publish >> "$TEST_FIXTURE/operations";cp "$TEST_FIXTURE/published.json" "$TEST_FIXTURE/release.json" ;;
   *) exit 9 ;;
  esac ;;
 attestation) [ "${TEST_ATTEST_FAIL:-0}" = 0 ] || exit 1
  case "$*" in *"--repo DKotsyuba/agent-run"*"--signer-workflow DKotsyuba/agent-run/.github/workflows/release.yml"*"--signer-digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"*"--source-digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"*"--source-ref refs/tags/$TEST_TAG"*) exit 0 ;; *) exit 9 ;; esac ;;
 *) exit 9 ;;
esac
"#,
        );
        Self {
            temp,
            root,
            assets,
            tools,
            manifest,
        }
    }

    /// Spawns the actual xtask binary with only local fixture routes.
    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
        command
            .current_dir(&self.root)
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    self.tools.display(),
                    std::env::var("PATH").unwrap()
                ),
            )
            .env("TEST_FIXTURE", self.temp.path())
            .env("TEST_TAG", &self.manifest.tag);
        command
    }

    /// Waits on the exact CLI child with a finite outer budget, killing/reaping on
    /// failure. Fixture helpers terminate promptly, so drained pipes cannot linger.
    fn output(&self, mut command: Command) -> Output {
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(25);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("finite release fixture exceeded outer budget");
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        child
            .stdout
            .take()
            .unwrap()
            .read_to_end(&mut stdout)
            .unwrap();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_end(&mut stderr)
            .unwrap();
        Output {
            status,
            stdout,
            stderr,
        }
    }

    /// Gives normal identity/integrity fixtures a finite ten-second observation budget.
    fn wait(&self, result: &Path, notify: Option<&Path>) -> Output {
        self.wait_for(result, notify, "10")
    }

    /// Uses an explicit deadline; the timeout boundary fixture alone requests one second.
    fn wait_for(&self, result: &Path, notify: Option<&Path>, timeout: &str) -> Output {
        let mut command = self.command();
        command
            .args([
                "release",
                "wait",
                "--repo",
                REPOSITORY,
                "--tag",
                &self.manifest.tag,
                "--commit",
                &self.manifest.commit,
                "--run-id",
                "17",
                "--attempt",
                "1",
                "--timeout",
                timeout,
                "--interval",
                "1",
                "--result-file",
            ])
            .arg(result);
        if let Some(path) = notify {
            command.arg("--notify-exec").arg(path);
        }
        self.output(command)
    }
}

/// Real observer control flow checks hashes/final identity and finite ACK handling.
#[test]
fn exact_release_and_wrong_notifier_ack_preserve_publication_fact() {
    let fixture = Fixture::new();
    fs::copy(
        fixture.temp.path().join("published.json"),
        fixture.temp.path().join("release.json"),
    )
    .unwrap();
    let result = fixture.temp.path().join("result.json");
    let output = fixture.wait(&result, None);
    assert!(
        output.status.success(),
        "stderr={} stdout={}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let event: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(event["status"], "released");
    assert_eq!(event["installed"], false);
    assert_eq!(event["agent_awakened"], false);
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(&result).unwrap()).unwrap(),
        event
    );
    let acknowledged = fixture.tools.join("ack");
    executable(
        &acknowledged,
        "#!/bin/sh\nid=$(jq -r .event_id)\nprintf '{\"event_id\":\"%s\",\"status\":\"accepted\"}' \"$id\"\n",
    );
    let ack_output = fixture.wait(
        &fixture.temp.path().join("ack-result.json"),
        Some(&acknowledged),
    );
    assert!(
        ack_output.status.success(),
        "{}",
        String::from_utf8_lossy(&ack_output.stdout)
    );
    let ack_event: Value = serde_json::from_slice(&ack_output.stdout).unwrap();
    assert_eq!(ack_event["event_id"], event["event_id"]);
    assert_eq!(ack_event["notification"]["status"], "acknowledged");
    let notifier = fixture.tools.join("notify");
    executable(
        &notifier,
        "#!/bin/sh\ncat >/dev/null\nprintf '{\"event_id\":\"wrong\",\"status\":\"accepted\"}'\n",
    );
    let output = fixture.wait(
        &fixture.temp.path().join("notify-result.json"),
        Some(&notifier),
    );
    assert_eq!(output.status.code(), Some(6));
    let event: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(event["status"], "released");
    assert_eq!(event["notification"]["status"], "failed");
    assert_eq!(fixture.wait(&result, None).status.code(), Some(5));
}

/// Workflow failure/changed attempt and absent release are different finite exits.
#[test]
fn workflow_failure_attempt_change_and_deadline_are_distinct() {
    let fixture = Fixture::new();
    let path = fixture.temp.path().join("run.json");
    let mut run: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    run["conclusion"] = json!("failure");
    record(&path, &run);
    assert_eq!(
        fixture
            .wait(&fixture.temp.path().join("failed.json"), None)
            .status
            .code(),
        Some(1)
    );
    run["conclusion"] = json!("success");
    run["run_attempt"] = json!(2);
    record(&path, &run);
    assert_eq!(
        fixture
            .wait(&fixture.temp.path().join("attempt.json"), None)
            .status
            .code(),
        Some(5)
    );
    run["run_attempt"] = json!(1);
    record(&path, &run);
    assert_eq!(
        fixture
            .wait_for(&fixture.temp.path().join("timeout.json"), None, "1")
            .status
            .code(),
        Some(2)
    );
}

/// Unauthenticated preflight cannot turn a private 404 into endless waiting;
/// lightweight and ambiguous annotated-run identities are terminal refusals.
#[test]
fn access_lightweight_and_ambiguous_discovery_fail_closed() {
    let fixture = Fixture::new();
    executable(
        &fixture.tools.join("gh"),
        "#!/bin/sh\nprintf 'HTTP/2.0 403 Forbidden\\r\\n\\r\\n{}'\nexit 1\n",
    );
    let output = fixture.wait(&fixture.temp.path().join("access.json"), None);
    assert_eq!(output.status.code(), Some(4));
    let fixture = Fixture::new();
    record(
        &fixture.temp.path().join("tag.json"),
        &json!({"ref":format!("refs/tags/{}",fixture.manifest.tag),"object":{"type":"commit","sha":fixture.manifest.commit}}),
    );
    assert_eq!(
        fixture
            .wait(&fixture.temp.path().join("lightweight.json"), None)
            .status
            .code(),
        Some(5)
    );
    let fixture = Fixture::new();
    let run: Value =
        serde_json::from_slice(&fs::read(fixture.temp.path().join("run.json")).unwrap()).unwrap();
    let mut second = run.clone();
    second["id"] = json!(18);
    record(
        &fixture.temp.path().join("runs.json"),
        &json!({"workflow_runs":[run,second]}),
    );
    let mut command = fixture.command();
    command
        .args([
            "release",
            "wait",
            "--repo",
            REPOSITORY,
            "--tag",
            &fixture.manifest.tag,
            "--commit",
            &fixture.manifest.commit,
            "--timeout",
            "10",
            "--result-file",
        ])
        .arg(fixture.temp.path().join("ambiguous.json"));
    let output = fixture.output(command);
    assert_eq!(output.status.code(), Some(5));
    assert!(String::from_utf8_lossy(&output.stdout).contains("ambiguous"));
}

/// Fault injection proves 503/429 recover on bounded reads, 401/ordinary403
/// never retry, malformed JSON fails distinctly and Retry-After respects deadline.
#[test]
fn transient_read_retry_auth_and_deadline_boundaries() {
    let fixture = Fixture::new();
    fs::copy(
        fixture.temp.path().join("published.json"),
        fixture.temp.path().join("release.json"),
    )
    .unwrap();
    let mut command = fixture.command();
    command
        .env("TEST_API_RETRY", "1")
        .args([
            "release",
            "wait",
            "--repo",
            REPOSITORY,
            "--tag",
            &fixture.manifest.tag,
            "--commit",
            &fixture.manifest.commit,
            "--run-id",
            "17",
            "--attempt",
            "1",
            "--timeout",
            "10",
            "--result-file",
        ])
        .arg(fixture.temp.path().join("retried.json"));
    let start = Instant::now();
    let output = fixture.output(command);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        start.elapsed() >= Duration::from_secs(1),
        "Retry-After must not be ignored"
    );
    assert_eq!(
        fs::read_to_string(fixture.temp.path().join("retry-count"))
            .unwrap()
            .trim(),
        "4"
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("UPSTREAM_CANARY"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("UPSTREAM_CANARY"));
    for (status, header, body, expected) in [
        (401, "", "{}", 4),
        (403, "", "{}", 4),
        (429, "Retry-After: 2\\r\\n", "{}", 2),
        (200, "", "malformed", 5),
    ] {
        let fixture = Fixture::new();
        let script = format!(
            r#"#!/bin/sh
printf 'called\n' >> "$TEST_FIXTURE/calls"
printf 'HTTP/2.0 {status} Response\r\n{header}\r\n{body}'
exit {exit}
"#,
            exit = if status == 200 { 0 } else { 1 }
        );
        executable(&fixture.tools.join("gh"), &script);
        // Only Retry-After tests the one-second deadline; other cases test refusal kinds.
        let timeout = if status == 429 { "1" } else { "10" };
        let output = fixture.wait_for(&fixture.temp.path().join("result.json"), None, timeout);
        assert_eq!(
            output.status.code(),
            Some(expected),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert_eq!(
            fs::read_to_string(fixture.temp.path().join("calls"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
}

/// Source preparation updates the descriptor from its explicit version plan,
/// preserving registration fields despite a differently versioned running xtask.
#[test]
fn release_prepare_updates_source_registration_mirror() {
    let fixture = Fixture::new();
    fs::write(fixture.root.join("Cargo.toml"),"[workspace]\n[workspace.package]\nversion = \"1.2.3\"\n[package]\nname = \"version-fixture\"\nversion.workspace = true\nedition = \"2024\"\n[lib]\npath = \"lib.rs\"\n").unwrap();
    fs::write(
        fixture.root.join("lib.rs"),
        "//! Disposable version fixture.\n",
    )
    .unwrap();
    fs::write(
        fixture.root.join("CHANGELOG.md"),
        "# Changes\n## Unreleased\nFixture release notes.\n",
    )
    .unwrap();
    fs::create_dir(fixture.root.join("schemas")).unwrap();
    let descriptor = json!({"schema_version":1,"product_version":"1.2.3","protocol_versions":["2026-07-28"],"operator":{"command":"preserved-command"}});
    record(
        &fixture.root.join("schemas/mcp-registration.json"),
        &descriptor,
    );
    let mut plan = fixture.command();
    plan.args(["release", "prepare", "1.2.4"]);
    let output = fixture.output(plan);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        plan["files"]
            .as_array()
            .unwrap()
            .contains(&json!("schemas/mcp-registration.json"))
    );
    assert_eq!(
        serde_json::from_slice::<Value>(
            &fs::read(fixture.root.join("schemas/mcp-registration.json")).unwrap()
        )
        .unwrap(),
        descriptor
    );
    let mut apply = fixture.command();
    apply.args(["release", "prepare", "1.2.4", "--apply"]);
    let output = fixture.output(apply);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut expected = descriptor;
    expected["product_version"] = json!("1.2.4");
    assert_eq!(
        serde_json::from_slice::<Value>(
            &fs::read(fixture.root.join("schemas/mcp-registration.json")).unwrap()
        )
        .unwrap(),
        expected
    );
    assert!(
        fs::read_to_string(fixture.root.join("Cargo.lock"))
            .unwrap()
            .contains("version = \"1.2.4\"")
    );
}

/// Stalled download/source leaves must start within the normal setup budget, and
/// the CLI must return before their thirty-second stubs can finish naturally
/// (this bounds CLI responsiveness only; the leaf's pipes are private to the CLI,
/// so it does not prove the leaf process died). Their result stays deadline
/// expiry; TERM during extraction stays cancelled, not integrity failure.
#[test]
fn stalled_leaves_and_cancelled_verification_keep_terminal_kind() {
    for stage in ["download", "source"] {
        let fixture = Fixture::new();
        fs::copy(
            fixture.temp.path().join("published.json"),
            fixture.temp.path().join("release.json"),
        )
        .unwrap();
        let mut command = fixture.command();
        command
            .env("TEST_STALL_STAGE", stage)
            .args([
                "release",
                "wait",
                "--repo",
                REPOSITORY,
                "--tag",
                &fixture.manifest.tag,
                "--commit",
                &fixture.manifest.commit,
                "--run-id",
                "17",
                "--attempt",
                "1",
                "--timeout",
                "10",
                "--result-file",
            ])
            .arg(fixture.temp.path().join("timeout.json"));
        let started = Instant::now();
        let output = fixture.output(command);
        assert!(
            started.elapsed() < Duration::from_secs(25),
            "CLI must return before the stalled leaf exits naturally"
        );
        assert!(
            fixture.temp.path().join("leaf-started").exists(),
            "actual {stage} leaf must run"
        );
        assert_eq!(
            output.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
    let fixture = Fixture::new();
    fs::copy(
        fixture.temp.path().join("published.json"),
        fixture.temp.path().join("release.json"),
    )
    .unwrap();
    executable(
        &fixture.tools.join("tar"),
        "#!/bin/sh\ntouch \"$TEST_FIXTURE/leaf-started\"\nsleep 4\nexit 1\n",
    );
    let result = fixture.temp.path().join("cancel.json");
    let mut command = fixture.command();
    command
        .args([
            "release",
            "wait",
            "--repo",
            REPOSITORY,
            "--tag",
            &fixture.manifest.tag,
            "--commit",
            &fixture.manifest.commit,
            "--run-id",
            "17",
            "--attempt",
            "1",
            "--timeout",
            "10",
            "--result-file",
        ])
        .arg(&result);
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(7);
    while !fixture.temp.path().join("leaf-started").exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    if !fixture.temp.path().join("leaf-started").exists() {
        let _ = child.kill();
        let _ = child.wait();
        panic!("verification extraction did not start");
    }
    // SAFETY: this is the still-owned unreaped CLI child, not a persisted PID.
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("cancelled verification exceeded outer budget");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(status.code(), Some(130));
    let event: Value = serde_json::from_slice(&fs::read(result).unwrap()).unwrap();
    assert_eq!(event["status"], "cancelled");
}

/// An owned foreground SIGINT produces a private cancelled receipt and reaps
/// its bounded API child; this makes no SIGKILL/crash-durability claim.
#[test]
fn controlled_interrupt_persists_cancelled_result() {
    let fixture = Fixture::new();
    executable(
        &fixture.tools.join("gh"),
        "#!/bin/sh\ntouch \"$TEST_FIXTURE/started\"\nsleep 3\nexit 1\n",
    );
    let result = fixture.temp.path().join("cancelled.json");
    let mut command = fixture.command();
    command
        .args([
            "release",
            "wait",
            "--repo",
            REPOSITORY,
            "--tag",
            &fixture.manifest.tag,
            "--commit",
            &fixture.manifest.commit,
            "--timeout",
            "10",
            "--result-file",
        ])
        .arg(&result);
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !fixture.temp.path().join("started").exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    if !fixture.temp.path().join("started").exists() {
        let _ = child.kill();
        let _ = child.wait();
        panic!("fixture API did not start");
    }
    // SAFETY: this owned child is unreaped, so its PID cannot have been reused.
    let signalled = unsafe { libc::kill(child.id() as i32, libc::SIGINT) } == 0;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("cancelled child did not finish");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(signalled);
    assert_eq!(status.code(), Some(130));
    let event: Value = serde_json::from_slice(&fs::read(result).unwrap()).unwrap();
    assert_eq!(event["status"], "cancelled");
    assert_eq!(event["installed"], false);
    assert_eq!(event["agent_awakened"], false);
}

/// The publisher workflow creates/verifies "dist" relative to its checkout.
/// Absolute and relative CLI paths must inspect/extract the same sealed bytes;
/// directory handling must retain regular-file/tamper/inventory refusals.
#[test]
fn package_create_verify_accept_relative_and_absolute_directories() {
    for absolute in [false, true] {
        let fixture = Fixture::new();
        let directory = fixture.root.join("dist");
        fs::create_dir(&directory).unwrap();
        for artifact in &fixture.manifest.artifacts {
            fs::copy(
                fixture.assets.join(&artifact.name),
                directory.join(&artifact.name),
            )
            .unwrap();
        }
        let argument = if absolute {
            directory.to_string_lossy().into_owned()
        } else {
            "dist".into()
        };
        let ci_command = || {
            let mut command = fixture.command();
            command
                .env("GITHUB_ACTIONS", "true")
                .env("GITHUB_REPOSITORY", REPOSITORY)
                .env("GITHUB_REPOSITORY_ID", REPOSITORY_ID.to_string())
                .env("GITHUB_SHA", &fixture.manifest.commit)
                .env("GITHUB_REF_NAME", &fixture.manifest.tag)
                .env("GITHUB_RUN_ID", "17")
                .env("GITHUB_RUN_ATTEMPT", "1")
                .env("AGENT_RUN_WORKFLOW_ID", WORKFLOW_ID.to_string());
            command
        };
        let mut create = ci_command();
        create.args([
            "package",
            "create",
            "--accepted-commit",
            &fixture.manifest.commit,
            "--directory",
            &argument,
        ]);
        let output = fixture.output(create);
        assert!(
            output.status.success(),
            "relative={}: {}",
            !absolute,
            String::from_utf8_lossy(&output.stderr)
        );
        for path in ["dist".to_owned(), directory.to_string_lossy().into_owned()] {
            let mut verify = fixture.command();
            verify.args([
                "package",
                "verify",
                "--accepted-commit",
                &fixture.manifest.commit,
                "--directory",
                &path,
                "--workflow-id",
                &WORKFLOW_ID.to_string(),
                "--run-id",
                "17",
                "--attempt",
                "1",
            ]);
            let output = fixture.output(verify);
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        fs::write(directory.join("install.sh"), b"tampered installer").unwrap();
        let mut verify = fixture.command();
        verify.args([
            "package",
            "verify",
            "--accepted-commit",
            &fixture.manifest.commit,
            "--directory",
            &argument,
            "--workflow-id",
            &WORKFLOW_ID.to_string(),
            "--run-id",
            "17",
            "--attempt",
            "1",
        ]);
        let output = fixture.output(verify);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("asset size/hash mismatch"));
    }
}

/// Publisher verifies its draft/downloads/provenance before publication, accepts
/// relative-directory publication and absolute-directory exact published no-op,
/// and refuses a conflicting existing draft or failed verifier. Draft tag reads
/// return GitHub's real 404 shape while list/ID reads expose the owned draft;
/// ambiguous, malformed, incomplete, changed and forbidden reads never write.
#[test]
fn staged_publisher_exact_noop_and_existing_draft_refusal() {
    let fixture = Fixture::new();
    let invoke = |directory: &Path| {
        let mut command = fixture.command();
        command
            .env("GITHUB_ACTIONS", "true")
            .env("GITHUB_REPOSITORY", REPOSITORY)
            .env("GITHUB_REPOSITORY_ID", REPOSITORY_ID.to_string())
            .env("GITHUB_SHA", &fixture.manifest.commit)
            .env("GITHUB_REF_NAME", &fixture.manifest.tag)
            .env("GITHUB_RUN_ID", "17")
            .env("GITHUB_RUN_ATTEMPT", "1")
            .env("AGENT_RUN_WORKFLOW_ID", WORKFLOW_ID.to_string());
        command
            .args([
                "release",
                "publish",
                "--accepted-commit",
                &fixture.manifest.commit,
                "--directory",
            ])
            .arg(directory)
            .arg("--notes")
            .arg(fixture.root.join("CHANGELOG.md"));
        command
    };
    let output = fixture.output(invoke(Path::new("../assets")));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(fixture.temp.path().join("operations")).unwrap(),
        "create\npublish\n"
    );
    let output = fixture.output(invoke(&fixture.assets));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(fixture.temp.path().join("operations")).unwrap(),
        "create\npublish\n"
    );
    fs::copy(
        fixture.temp.path().join("draft.json"),
        fixture.temp.path().join("release.json"),
    )
    .unwrap();
    let output = fixture.output(invoke(Path::new("../assets")));
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("existing draft"));
    let draft: Value =
        serde_json::from_slice(&fs::read(fixture.temp.path().join("draft.json")).unwrap()).unwrap();
    let mut unrelated = draft.clone();
    unrelated["tag_name"] = json!("v0.0.0");
    let mut zero_id = draft.clone();
    zero_id["id"] = json!(0);
    for (list, reason) in [
        (json!([zero_id]), "listed release ID invalid"),
        (json!([draft.clone(), draft.clone()]), "ambiguous releases"),
        (json!({}), "release list invalid"),
        (json!(vec![unrelated; 100]), "release pagination limit"),
    ] {
        record(&fixture.temp.path().join("releases.json"), &list);
        let output = fixture.output(invoke(Path::new("../assets")));
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(reason));
    }
    record(
        &fixture.temp.path().join("releases.json"),
        &json!([draft.clone()]),
    );
    for (field, value) in [("id", json!(30)), ("tag_name", json!("v0.0.0"))] {
        let mut changed = draft.clone();
        changed[field] = value;
        record(&fixture.temp.path().join("release-id.json"), &changed);
        let output = fixture.output(invoke(Path::new("../assets")));
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("listed release identity changed")
        );
    }
    let mut denied = invoke(Path::new("../assets"));
    denied.env("TEST_RELEASE_LIST_DENIED", "1");
    let output = fixture.output(denied);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("authentication/access denied"));
    let mut command = invoke(Path::new("../assets"));
    command.env("TEST_ATTEST_FAIL", "1");
    let output = fixture.output(command);
    assert!(!output.status.success());
    assert_eq!(
        fs::read_to_string(fixture.temp.path().join("operations")).unwrap(),
        "create\npublish\n"
    );
}
