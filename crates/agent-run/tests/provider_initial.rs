#![cfg(feature = "test-fixtures")]
//! New provider admission through the real detached supervisor and fake engine.

use agent_run::{
    domain::{AgentId, Status},
    service::Service,
    state::Store,
};
use agent_run_domain::{
    catalog::{AccountRecord, AccountStatus, QuotaCandidate, QuotaCandidateSet, SelectionIntent},
    AccountId, PositiveFinite, ProviderStartRequest,
};
use std::{
    fs,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    path::Path,
    process::Stdio,
    time::Duration,
};
use tokio::process::Command;

/// Duplicates one inert test descriptor above the supervisor bootstrap range.
fn bootstrap_fd() -> OwnedFd {
    let file = fs::File::options()
        .read(true)
        .write(true)
        .open("/dev/null")
        .unwrap();
    // SAFETY: F_DUPFD_CLOEXEC returns a new descriptor owned by this test.
    let fd = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 10) };
    assert!(fd >= 0);
    // SAFETY: the successful fcntl result has one owner.
    unsafe { OwnedFd::from_raw_fd(fd) }
}

/// Starts the ordinary agent-run supervisor executable with its three
/// inherited bootstrap descriptors and one synthetic environment token.
fn supervisor(home: &Path, id: &AgentId) -> tokio::process::Child {
    use std::os::unix::process::CommandExt;

    let ready = bootstrap_fd();
    let identity = bootstrap_fd();
    let error = bootstrap_fd();
    let sources = [ready.as_raw_fd(), identity.as_raw_fd(), error.as_raw_fd()];
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-run"));
    command
        .arg("--home")
        .arg(home)
        .args([
            "_supervisor",
            "--ready-fd",
            "3",
            "--identity-fd",
            "4",
            "--error-fd",
            "5",
            id.as_str(),
        ])
        .env("HOME", home)
        .env("FAKE_TOKEN", "synthetic-token")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: setsid and dup2 are async-signal-safe and use test-owned fds.
    unsafe {
        command.as_std_mut().pre_exec(move || {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            for (source, target) in sources.into_iter().zip([3, 4, 5]) {
                if libc::dup2(source, target) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    command.kill_on_drop(true).spawn().unwrap()
}

/// Creates a disposable schema-v2 home with one arbitrary Claude Messages
/// provider and one registered fake environment reference.
fn home() -> (tempfile::TempDir, std::path::PathBuf) {
    let temp = tempfile::Builder::new()
        .prefix("ar-provider-")
        .tempdir_in(std::env::var_os("AGENT_RUN_TEST_TMP").unwrap_or_else(|| "/tmp".into()))
        .unwrap();
    let root = temp.path().canonicalize().unwrap();
    agent_run::fs::private_dir(&root).unwrap();
    Store::initialize(&root).unwrap();
    fs::create_dir_all(root.join("profiles")).unwrap();
    fs::write(root.join("profiles/review.md"),
        "+++\nrevision = \"1\"\nwrite = false\nnetwork = false\nallow_external_read_roots = false\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nReview safely.\n").unwrap();
    let binary = env!("CARGO_BIN_EXE_agent-run-fixture");
    fs::write(
        root.join("config.toml"),
        format!(
            r#"
schema_version = 2
[harnesses.codex]
binary = "{binary}"
home = "{root}/codex"
[harnesses.claude-code]
binary = "{binary}"
home = "{root}/claude"
[providers.glm-user]
harness = "claude-code"
connection = {{ kind = "custom", endpoint = "https://gateway.example/api", protocol = "messages" }}
auth_family = "anthropic"
limits_source = "none"
[[providers.glm-user.models]]
id = "fixture"
native_model = "fixture"
[[providers.glm-user.bindings]]
label = "work"
account = "acct-work"
"#,
            root = root.display()
        ),
    )
    .unwrap();
    Store::open(&root)
        .unwrap()
        .register_account(&AccountRecord {
            account_id: "acct-work".parse().unwrap(),
            auth_family: "anthropic".parse().unwrap(),
            secret_ref: "env:FAKE_TOKEN".parse().unwrap(),
            status: AccountStatus::Enabled,
        })
        .unwrap();
    (temp, root)
}

/// Returns one strict provider request with an explicit local account pin.
fn request(home: &Path) -> ProviderStartRequest {
    serde_json::from_value(serde_json::json!({
        "provider":"glm-user","model":"fixture","profile":"review",
        "task":"fixture:answer","workdir":home,"account":"work","request_id":"provider-1",
        "orchestrator":{"transport":"fixture","external_session_id":"test-session"}
    }))
    .unwrap()
}

/// Supplies the fixed trusted candidate; user request JSON has no candidate
/// field and the store rechecks this revision under BEGIN IMMEDIATE.
fn candidates(revision: i64) -> QuotaCandidateSet {
    let account: AccountId = "acct-work".parse().unwrap();
    QuotaCandidateSet {
        provider: "glm-user".parse().unwrap(),
        model: "fixture".into(),
        intent: SelectionIntent::Pinned(account.clone()),
        candidates: vec![QuotaCandidate {
            account: account.clone(),
            rank: 0,
            physical_keys: vec![
                agent_run_domain::PhysicalQuotaKey::new(&account, "tokens").unwrap()
            ],
            multiplier: PositiveFinite::try_from(1.0).unwrap(),
        }],
        capacity_revision: revision,
    }
}

/// The real supervisor keeps the chosen attempt through transcript, process
/// proof, sealed answer, terminal cleanup and exactly one delivery notice.
#[tokio::test]
async fn provider_start_completes_one_owned_fake_engine_attempt() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let request = request(&home);
    let admitted = service
        .admit_provider_trusted(request.clone(), candidates(0))
        .unwrap();
    assert_eq!(admitted["created"], true);
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    let attempt = admitted["attempt_id"].as_str().unwrap().to_owned();
    let mut child = supervisor(&home, &id);
    let exit = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(exit.success(), "supervisor exit: {exit}");
    let store = Store::open(&home).unwrap();
    let row = store.get(&id).unwrap();
    assert_eq!(row.status, Status::Succeeded);
    assert_eq!(
        service.answer(&id).unwrap()["content"],
        "fixture final answer\n"
    );
    let (selected, active, cleanup, process): (String, i64, Option<String>, Option<String>) = store
        .conn
        .query_row(
            "SELECT selected_account_id,ownership_active,cleanup_proof_json,process_identity \
             FROM attempts WHERE id=?",
            [&attempt],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(selected, "acct-work");
    assert_eq!(active, 0);
    assert!(cleanup.is_some() && process.is_some());
    let attached: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE agent_id=? AND attempt_id=?",
            rusqlite::params![id.as_str(), attempt],
            |row| row.get(0),
        )
        .unwrap();
    assert!(attached >= 2);
    let assistant_text: String = store
        .conn
        .query_row(
            "SELECT COALESCE(group_concat(content, ''),'') FROM messages \
             WHERE agent_id=? AND attempt_id=? AND role='assistant'",
            rusqlite::params![id.as_str(), attempt],
            |row| row.get(0),
        )
        .unwrap();
    assert!(assistant_text.contains("fixture partial"));
    let deliveries: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(deliveries, 1);
    fs::write(home.join("config.toml"), "schema_version=2\n").unwrap();
    let replay = service
        .admit_provider_trusted(request, candidates(0))
        .unwrap();
    assert_eq!(replay["created"], false);
    assert_eq!(replay["agent_id"], admitted["agent_id"]);
}

/// Automatic selection freezes the eligible scope without inventing a
/// provider-local account pin or a fixed global login reference.
#[tokio::test]
async fn provider_auto_start_uses_trusted_candidate_and_completes() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let mut request = request(&home);
    request.account = None;
    request.request_id = Some("provider-auto".into());
    let mut selected = candidates(0);
    selected.intent = SelectionIntent::Auto;
    let admitted = service.admit_provider_trusted(request, selected).unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    let store = Store::open(&home).unwrap();
    assert_eq!(
        store.get(&id).unwrap().identity.unwrap()["authority"]["role_payload"]["auth"]["mode"],
        "global"
    );
    assert_eq!(store.provider_attempt(&id).unwrap().1.as_str(), "acct-work");
    let mut child = supervisor(&home, &id);
    assert!(tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .unwrap()
        .unwrap()
        .success());
    assert_eq!(
        Store::open(&home).unwrap().get(&id).unwrap().status,
        Status::Succeeded
    );
}

/// Cancellation queued before spawn closes the exact owned attempt with
/// never-spawned proof and no fabricated process identity or answer.
#[tokio::test]
async fn provider_cancel_before_spawn_releases_only_its_attempt() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let admitted = service
        .admit_provider_trusted(request(&home), candidates(0))
        .unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    Store::open(&home)
        .unwrap()
        .enqueue(&id, "cancel", &serde_json::json!({}))
        .unwrap();
    let mut child = supervisor(&home, &id);
    let exit = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(exit.success());
    let store = Store::open(&home).unwrap();
    assert_eq!(store.get(&id).unwrap().status, Status::Cancelled);
    let (active, proof, process): (i64, Option<String>, Option<String>) = store.conn.query_row(
        "SELECT ownership_active,cleanup_proof_json,process_identity FROM attempts WHERE agent_id=?",
        [id.as_str()], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
    ).unwrap();
    assert_eq!(active, 0);
    assert_eq!(proof.as_deref(), Some("{\"never_spawned\":true}"));
    assert_eq!(process, None);
    assert_eq!(service.answer(&id).unwrap()["available"], false);
}

/// A changed sealed role cannot acquire different grants on retry; the
/// pre-spawn failure closes the owned attempt without a process identity.
#[tokio::test]
async fn provider_supervisor_refuses_tampered_role_authority() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let admitted = service
        .admit_provider_trusted(request(&home), candidates(0))
        .unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    let store = Store::open(&home).unwrap();
    let mut identity = store.get(&id).unwrap().identity.unwrap();
    identity["authority"]["role_payload"]["grants"]["network"] = serde_json::json!(true);
    store
        .conn
        .execute(
            "UPDATE agents SET identity_json=? WHERE id=?",
            rusqlite::params![identity.to_string(), id.as_str()],
        )
        .unwrap();
    let mut child = supervisor(&home, &id);
    let _ = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .unwrap()
        .unwrap();
    let store = Store::open(&home).unwrap();
    assert_eq!(store.get(&id).unwrap().status, Status::Failed);
    let (active, proof, process): (i64, Option<String>, Option<String>) = store
        .conn
        .query_row(
            "SELECT ownership_active,cleanup_proof_json,process_identity FROM attempts WHERE agent_id=?",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(active, 0);
    assert_eq!(proof.as_deref(), Some("{\"never_spawned\":true}"));
    assert_eq!(process, None);
}

/// An OS refusal to spawn the configured engine proves no child existed and
/// releases the first attempt without pretending that a process was cleaned.
#[tokio::test]
async fn provider_spawn_error_closes_owned_attempt_without_process() {
    let (_temp, home) = home();
    let path = home.join("config.toml");
    let config = fs::read_to_string(&path).unwrap();
    let missing = home.join("missing-engine");
    fs::write(
        &path,
        config.replace(
            env!("CARGO_BIN_EXE_agent-run-fixture"),
            missing.to_str().unwrap(),
        ),
    )
    .unwrap();
    let service = Service::new(home.clone());
    let admitted = service
        .admit_provider_trusted(request(&home), candidates(0))
        .unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    let mut child = supervisor(&home, &id);
    let _ = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .unwrap()
        .unwrap();
    let store = Store::open(&home).unwrap();
    assert_eq!(store.get(&id).unwrap().status, Status::Failed);
    let (active, proof, process): (i64, Option<String>, Option<String>) = store
        .conn
        .query_row(
            "SELECT ownership_active,cleanup_proof_json,process_identity FROM attempts WHERE agent_id=?",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(active, 0);
    assert_eq!(proof.as_deref(), Some("{\"never_spawned\":true}"));
    assert_eq!(process, None);
}
