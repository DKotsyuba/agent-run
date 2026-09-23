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
use rusqlite::OptionalExtension;
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
    home_with("", &[])
}

/// [`home`] plus `extra` TOML appended to the config (more bindings,
/// providers, or core caps) and more enabled fake-token accounts.
fn home_with(extra: &str, accounts: &[&str]) -> (tempfile::TempDir, std::path::PathBuf) {
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
{extra}"#,
            root = root.display()
        ),
    )
    .unwrap();
    let mut store = Store::open(&root).unwrap();
    for (index, id) in std::iter::once("acct-work")
        .chain(accounts.iter().copied())
        .enumerate()
    {
        // Each account needs its own reference; only the first is ever run.
        let reference = match index {
            0 => "env:FAKE_TOKEN".to_owned(),
            n => format!("env:FAKE_TOKEN_{n}"),
        };
        store
            .register_account(&AccountRecord {
                account_id: id.parse().unwrap(),
                auth_family: "anthropic".parse().unwrap(),
                secret_ref: reference.parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
    }
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
            quota_known: true,
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
    let stored_identity = Store::open(&home)
        .unwrap()
        .get(&id)
        .unwrap()
        .identity
        .unwrap()
        .to_string();
    assert!(!stored_identity.contains("synthetic-token"));
    assert!(!stored_identity.contains("env:FAKE_TOKEN"));
    fs::write(home.join("config.toml"), "schema_version=2\n").unwrap();
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

/// A changed stored provider setting without the original snapshot digest
/// cannot alter the model or executable after admission.
#[tokio::test]
async fn provider_supervisor_refuses_tampered_config_snapshot() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let admitted = service
        .admit_provider_trusted(request(&home), candidates(0))
        .unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    let store = Store::open(&home).unwrap();
    let mut identity = store.get(&id).unwrap().identity.unwrap();
    identity["provider_config"]["providers"]["glm-user"]["models"][0]["native_model"] =
        serde_json::json!("different-native-model");
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
    let (active, process): (i64, Option<String>) = store
        .conn
        .query_row(
            "SELECT ownership_active,process_identity FROM attempts WHERE agent_id=?",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((active, process), (0, None));
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

/// A second enabled account on `glm-user` and an alias provider binding the
/// first account again under another label.
const TWO_ACCOUNTS: &str = r#"
[[providers.glm-user.bindings]]
label = "alt"
account = "acct-alt"
[providers.glm-alias]
harness = "claude-code"
connection = { kind = "custom", endpoint = "https://gateway.example/api", protocol = "messages" }
auth_family = "anthropic"
limits_source = "none"
[[providers.glm-alias.models]]
id = "fixture"
native_model = "fixture"
[[providers.glm-alias.bindings]]
label = "shared"
account = "acct-work"
"#;

/// One strict request on `provider` with `request_id` and optional pin label.
fn request_for(
    home: &Path,
    provider: &str,
    request_id: &str,
    pin: Option<&str>,
) -> ProviderStartRequest {
    let mut request = request(home);
    request.provider = provider.parse().unwrap();
    request.request_id = Some(request_id.into());
    request.account = pin.map(|label| label.parse().unwrap());
    request
}

/// Returns the admitted attempt's selected global account.
fn selected(home: &Path, admitted: &serde_json::Value) -> String {
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    Store::open(home)
        .unwrap()
        .provider_attempt(&id)
        .unwrap()
        .1
        .as_str()
        .to_owned()
}

/// Counts `(agents, attempts)` rows, proving what was (not) admitted.
fn rows(home: &Path) -> (i64, i64) {
    let store = Store::open(home).unwrap();
    let count = |table: &str| {
        store
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    };
    (count("agents"), count("attempts"))
}

/// Advances the committed quota revision, as a concurrent collector would.
fn bump_revision(home: &Path) -> agent_run_domain::Result<()> {
    let mut store = Store::open(home)?;
    let tx = store.conn.transaction()?;
    Store::advance_quota_capacity_revision(&tx)?;
    tx.commit()?;
    Ok(())
}

/// The ordinary entry computes its own candidates from persisted evidence
/// and the admitted attempt completes through the real supervisor.
#[tokio::test]
async fn provider_ordinary_admission_ranks_itself_and_completes() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let admitted = service.admit_provider(request(&home)).unwrap();
    assert_eq!(admitted["created"], true);
    assert_eq!(selected(&home, &admitted), "acct-work");
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
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

/// Replay returns the original admission before config, registry or quota
/// is read; a different request under the same id is a typed conflict.
#[tokio::test]
async fn provider_replay_precedes_changed_config_and_quota() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let original = request_for(&home, "glm-user", "replay-1", None);
    let admitted = service.admit_provider(original.clone()).unwrap();
    bump_revision(&home).unwrap();
    Store::open(&home)
        .unwrap()
        .disable_account(&"acct-work".parse().unwrap())
        .unwrap();
    fs::write(home.join("config.toml"), "schema_version=2\n").unwrap();
    let replay = service
        .admit_provider_observed(original.clone(), &mut |_| {
            panic!("replay must not rank or submit")
        })
        .unwrap();
    assert_eq!(replay["created"], false);
    assert_eq!(replay["agent_id"], admitted["agent_id"]);
    let mut conflicting = original;
    conflicting.task = "fixture:other".into();
    assert!(matches!(
        service.admit_provider(conflicting),
        Err(agent_run_domain::Error::Conflict)
    ));
    assert_eq!(rows(&home), (1, 1));
}

/// A revision moved between ranking and admission is recomputed; a
/// revision that keeps moving stops after exactly one initial selection
/// plus three recalculations with `selection_busy` and no rows.
#[tokio::test]
async fn provider_stale_selection_recomputes_at_most_three_times() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let mut seen = Vec::new();
    let admitted = service
        .admit_provider_observed(
            request_for(&home, "glm-user", "stale-once", None),
            &mut |n| {
                seen.push(n);
                if n == 0 {
                    bump_revision(&home)?;
                }
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(admitted["created"], true);
    assert_eq!(seen, [0, 1]);

    let mut seen = Vec::new();
    let busy = service
        .admit_provider_observed(
            request_for(&home, "glm-user", "stale-always", None),
            &mut |n| {
                seen.push(n);
                bump_revision(&home)
            },
        )
        .unwrap_err();
    assert_eq!(seen, [0, 1, 2, 3]);
    assert!(matches!(
        busy,
        agent_run_domain::Error::QuotaAdmission(
            agent_run_domain::catalog::QuotaAdmissionError::SelectionBusy { stale_retries: 3 }
        )
    ));
    assert_eq!(busy.machine_code().as_str(), "selection_busy");
    assert_eq!(rows(&home), (1, 1), "selection_busy admitted nothing");
}

/// A pin never falls through to another account, while auto selection
/// skips a disabled account; a disable racing the transaction is caught by
/// the store's current-registry guard.
#[tokio::test]
async fn provider_pin_and_disable_race_never_fall_through() {
    let (_temp, home) = home_with(TWO_ACCOUNTS, &["acct-alt"]);
    let service = Service::new(home.clone());
    let raced = service.admit_provider_observed(
        request_for(&home, "glm-user", "race", Some("work")),
        &mut |_| Store::open(&home)?.disable_account(&"acct-work".parse().unwrap()),
    );
    let code = |result: agent_run_domain::Result<serde_json::Value>| {
        result.unwrap_err().machine_code().as_str()
    };
    assert_eq!(code(raced), "no_eligible_account");
    assert_eq!(rows(&home), (0, 0));
    let pinned = service.admit_provider(request_for(&home, "glm-user", "pinned", Some("work")));
    assert_eq!(code(pinned), "no_eligible_account");
    assert_eq!(rows(&home), (0, 0), "a pin never falls over to acct-alt");
    let auto = service
        .admit_provider(request_for(&home, "glm-user", "auto", None))
        .unwrap();
    assert_eq!(selected(&home, &auto), "acct-alt");
}

/// Equal quota ranks break ties by active load then id, and an alias label
/// on another provider shares its global account's load.
#[tokio::test]
async fn provider_equal_ranks_balance_load_across_aliases() {
    let (_temp, home) = home_with(TWO_ACCOUNTS, &["acct-alt"]);
    let service = Service::new(home.clone());
    let alias = service
        .admit_provider(request_for(&home, "glm-alias", "alias", Some("shared")))
        .unwrap();
    assert_eq!(selected(&home, &alias), "acct-work");
    let first = service
        .admit_provider(request_for(&home, "glm-user", "auto-1", None))
        .unwrap();
    assert_eq!(
        selected(&home, &first),
        "acct-alt",
        "acct-work already busy"
    );
    let second = service
        .admit_provider(request_for(&home, "glm-user", "auto-2", None))
        .unwrap();
    assert_eq!(selected(&home, &second), "acct-alt", "equal load: lower id");
    let third = service
        .admit_provider(request_for(&home, "glm-user", "auto-3", None))
        .unwrap();
    assert_eq!(selected(&home, &third), "acct-work", "lower load wins");
}

/// Concurrent submissions never double-create one request id and never
/// exceed the global active cap.
#[tokio::test]
async fn provider_concurrent_admissions_respect_replay_and_caps() {
    let (_temp, home) = home_with("[core]\nmax_active_agents = 2\n", &[]);
    let service = Service::new(home.clone());
    let same = request_for(&home, "glm-user", "same", None);
    let outcomes: Vec<_> = (0..6)
        .map(|_| {
            let (service, request) = (service.clone(), same.clone());
            std::thread::spawn(move || service.admit_provider(request))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|handle| handle.join().unwrap().unwrap())
        .collect();
    assert_eq!(outcomes.iter().filter(|o| o["created"] == true).count(), 1);
    assert!(outcomes
        .iter()
        .all(|o| o["agent_id"] == outcomes[0]["agent_id"]));
    assert_eq!(rows(&home), (1, 1));
    let distinct: Vec<_> = (0..6)
        .map(|n| {
            let (service, request) = (
                service.clone(),
                request_for(&home, "glm-user", &format!("cap-{n}"), None),
            );
            std::thread::spawn(move || service.admit_provider(request))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(distinct.iter().filter(|o| o.is_ok()).count(), 1);
    assert!(distinct
        .iter()
        .filter_map(|o| o.as_ref().err())
        .all(|error| matches!(error, agent_run_domain::Error::Capacity)));
    assert_eq!(rows(&home), (2, 2));
}

/// Harness options follow the harness: fast on a claude-code provider is a
/// validation error with no row, while an output_schema request is admitted
/// and completes through the real supervisor.
#[tokio::test]
async fn provider_harness_options_are_validated_and_carried() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let mut fast = request_for(&home, "glm-user", "fast", None);
    fast.fast = true;
    let refused = service.admit_provider(fast).unwrap_err();
    assert_eq!(refused.machine_code().as_str(), "ValidationError");
    assert_eq!(rows(&home), (0, 0));
    let mut schema = request_for(&home, "glm-user", "schema", None);
    schema.output_schema = serde_json::json!({"type": "object"}).as_object().cloned();
    let admitted = service.admit_provider(schema).unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
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

/// Runs one admitted provider agent to its terminal state through the real
/// supervisor executable (bounded to 20 s).
async fn run_to_end(home: &Path, id: &AgentId) {
    let mut child = supervisor(home, id);
    let exit = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(exit.success(), "supervisor exit: {exit}");
}

/// Returns the parsed adapter state of `id`'s latest attempt.
fn attempt_state(home: &Path, id: &AgentId) -> serde_json::Value {
    let text: String = Store::open(home)
        .unwrap()
        .conn
        .query_row(
            "SELECT adapter_state_json FROM attempts WHERE agent_id=? ORDER BY number DESC LIMIT 1",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&text).unwrap()
}

/// Writes the Claude native transcript for `session` under the run's
/// custom-gateway config directory, as the harness itself would.
fn transcript(runtime_home: &Path, session: &str, body: &str) {
    let dir = runtime_home.join("claude-config/projects/-fixture-workdir");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(format!("{session}.jsonl")), body).unwrap();
}

/// Explicit provider resume: unprovable native history refuses with
/// `continuation_unavailable` and admits nothing; a proven history admits one
/// child that keeps the parent's authority, assets, runtime home and native
/// session, replays by request id, refuses a second child, and resumes the
/// same native session through the real supervisor. A pinned account that
/// became unavailable is never switched away from.
#[tokio::test]
async fn provider_resume_continues_the_proven_native_session() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let admitted = service
        .admit_provider_trusted(request(&home), candidates(0))
        .unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    run_to_end(&home, &id).await;
    let parent = Store::open(&home).unwrap().get(&id).unwrap();
    assert_eq!(parent.status, Status::Succeeded);
    let session = parent.runtime_session_id.clone().unwrap();
    let runtime_home = std::path::PathBuf::from(
        parent.identity.as_ref().unwrap()["runtime_home"]
            .as_str()
            .unwrap(),
    );
    let orchestrator: agent_run::domain::OrchestratorRef = serde_json::from_value(
        serde_json::json!({"transport":"fixture","external_session_id":"test-session"}),
    )
    .unwrap();
    let resume = |request_id: &str| {
        service.admit_provider_resume(
            &Store::open(&home).unwrap().get(&id).unwrap(),
            "fixture:answer".into(),
            None,
            Some(request_id.into()),
            Some(orchestrator.clone()),
        )
    };
    let children = || -> i64 {
        Store::open(&home)
            .unwrap()
            .conn
            .query_row(
                "SELECT COUNT(*) FROM agents WHERE parent_agent_id=?",
                [id.as_str()],
                |row| row.get(0),
            )
            .unwrap()
    };
    // The supervisor sealed the history the harness wrote before completing.
    let state = attempt_state(&home, &id);
    let seal = &state["native_history"]["seal"];
    assert_eq!(seal["session"], session.as_str(), "{state}");
    assert_eq!(seal["records"], 4, "{state}");
    let history = runtime_home.join(format!(
        "claude-config/projects/-fixture-workdir/{session}.jsonl"
    ));
    let sealed = fs::read(&history).unwrap();
    // A same-session rewrite (last complete line dropped) is detected.
    let text = String::from_utf8(sealed.clone()).unwrap();
    let kept: String = text
        .lines()
        .take(3)
        .map(|line| format!("{line}\n"))
        .collect();
    fs::write(&history, kept).unwrap();
    let refused = resume("resume-1").unwrap_err().to_string();
    assert!(refused.contains("changed since it was sealed"), "{refused}");
    fs::write(&history, &sealed).unwrap();
    // A parent without a recorded seal fails closed; the history present on
    // disk is never adopted as a new baseline.
    let recorded = serde_json::to_string(&state).unwrap();
    let set_state = |value: &str| {
        Store::open(&home)
            .unwrap()
            .conn
            .execute(
                "UPDATE attempts SET adapter_state_json=? WHERE agent_id=?",
                rusqlite::params![value, id.as_str()],
            )
            .unwrap();
    };
    set_state("{}");
    let refused = resume("resume-1").unwrap_err().to_string();
    assert!(
        refused.contains("recorded no native history seal"),
        "{refused}"
    );
    set_state(&recorded);
    assert_eq!(children(), 0, "a refusal admits nothing");

    let child = resume("resume-1").unwrap();
    assert_eq!(child["created"], true, "{child}");
    let child_id: AgentId = serde_json::from_value(child["agent_id"].clone()).unwrap();
    let row = Store::open(&home).unwrap().get(&child_id).unwrap();
    assert_eq!(row.parent_agent_id.as_ref(), Some(&id));
    assert_eq!(row.root_agent_id, id);
    assert_eq!(row.sequence, 2);
    assert_eq!(
        row.resume_of_runtime_session_id.as_deref(),
        Some(session.as_str())
    );
    let (parent_identity, child_identity) = (
        parent.identity.clone().unwrap(),
        row.identity.clone().unwrap(),
    );
    for key in ["runtime_home", "snapshot_sha256", "provider_config_sha256"] {
        assert_eq!(parent_identity[key], child_identity[key], "{key}");
    }
    assert_eq!(
        parent_identity["authority"]["assets_sha256"],
        child_identity["authority"]["assets_sha256"]
    );
    assert_eq!(
        parent_identity["authority"]["role_payload"],
        child_identity["authority"]["role_payload"]
    );
    let replay = resume("resume-1").unwrap();
    assert_eq!(replay["created"], false);
    assert_eq!(replay["agent_id"], child["agent_id"]);
    let second = resume("resume-2").unwrap_err().to_string();
    assert!(second.contains("already been resumed"), "{second}");
    assert_eq!(children(), 1);

    run_to_end(&home, &child_id).await;
    let row = Store::open(&home).unwrap().get(&child_id).unwrap();
    assert_eq!(row.status, Status::Succeeded, "{:?}", row.failure_text);
    assert_eq!(row.runtime_session_id.as_deref(), Some(session.as_str()));
    let (selected, cleanup): (String, Option<String>) = Store::open(&home)
        .unwrap()
        .conn
        .query_row(
            "SELECT selected_account_id,cleanup_proof_json FROM attempts WHERE agent_id=?",
            [child_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(selected, "acct-work");
    assert!(cleanup.is_some());
    // After the child ran (and appended to the native history), a second
    // resume of the parent is refused as already resumed.
    let again = resume("resume-4").unwrap_err().to_string();
    assert!(again.contains("already been resumed"), "{again}");
    // The child's own attempt sealed the continued history (both turns).
    assert_eq!(
        attempt_state(&home, &child_id)["native_history"]["seal"]["records"],
        8
    );

    // The pinned account is disabled: resume refuses instead of switching.
    Store::open(&home)
        .unwrap()
        .disable_account(&"acct-work".parse().unwrap())
        .unwrap();
    let pinned = service
        .admit_provider_resume(
            &Store::open(&home).unwrap().get(&child_id).unwrap(),
            "fixture:answer".into(),
            None,
            Some("resume-3".into()),
            Some(orchestrator.clone()),
        )
        .unwrap_err()
        .to_string();
    assert!(!pinned.contains("continuation_unavailable"), "{pinned}");
    let grandchildren: i64 = Store::open(&home)
        .unwrap()
        .conn
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE parent_agent_id=?",
            [child_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(grandchildren, 0, "{pinned}");
}

/// A newly required unsupported isolation boundary must revoke continuation,
/// even when the provider and public model names remain unchanged.
#[tokio::test]
async fn provider_resume_honors_new_model_restrictions() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let original = request(&home);
    let provider = original.provider.to_string();
    let model = original.model.clone();
    let admitted = service
        .admit_provider_trusted(original, candidates(0))
        .unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    run_to_end(&home, &id).await;
    let parent = Store::open(&home).unwrap().get(&id).unwrap();
    let session = parent.runtime_session_id.as_deref().unwrap();
    let runtime_home = std::path::PathBuf::from(
        parent.identity.as_ref().unwrap()["runtime_home"]
            .as_str()
            .unwrap(),
    );
    transcript(
        &runtime_home,
        session,
        &format!("{{\"sessionId\":\"{session}\"}}\n"),
    );
    let path = home.join("config.toml");
    let mut config: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    let offering = config["providers"][&provider]["models"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|offering| offering["id"].as_str() == Some(model.as_str()))
        .unwrap();
    offering.as_table_mut().unwrap().insert(
        "restrictions".into(),
        toml::Value::Array(vec![toml::Value::String(
            "filesystem_read_isolation".into(),
        )]),
    );
    fs::write(path, toml::to_string(&config).unwrap()).unwrap();
    let resumed = service.admit_provider_resume(
        &parent,
        "fixture:answer".into(),
        None,
        Some("revoked-resume".into()),
        None,
    );
    assert!(
        resumed.is_err(),
        "resume ignored current model restrictions"
    );
    assert_eq!(rows(&home), (1, 1), "a refused resume creates no child");
}

/// Two concurrent explicit resumes of one parent (distinct request ids)
/// admit exactly one child; the loser is refused and nothing else is written.
#[tokio::test]
async fn concurrent_provider_resumes_admit_one_child() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let admitted = service
        .admit_provider_trusted(request(&home), candidates(0))
        .unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    run_to_end(&home, &id).await;
    let parent = Store::open(&home).unwrap().get(&id).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let results: Vec<bool> = (0..2)
        .map(|index| {
            let (home, barrier, parent) = (home.clone(), barrier.clone(), parent.clone());
            std::thread::spawn(move || {
                let service = Service::new(home);
                barrier.wait();
                service
                    .admit_provider_resume(
                        &parent,
                        "fixture:answer".into(),
                        None,
                        Some(format!("race-{index}")),
                        Some(
                            serde_json::from_value(serde_json::json!({
                                "transport":"fixture","external_session_id":"test-session"
                            }))
                            .unwrap(),
                        ),
                    )
                    .is_ok()
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|ok| **ok).count(), 1, "{results:?}");
    let children: i64 = Store::open(&home)
        .unwrap()
        .conn
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE parent_agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(children, 1);
}

/// Runs one pinned parent to completion and returns its id.
async fn completed_parent(home: &Path, service: &Service, request_id: &str) -> AgentId {
    let mut request = request(home);
    request.request_id = Some(request_id.into());
    let revision = Store::open(home)
        .unwrap()
        .quota_capacity_revision()
        .unwrap();
    let admitted = service
        .admit_provider_trusted(request, candidates(revision))
        .unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    run_to_end(home, &id).await;
    id
}

/// Rewrites config.toml through `edit` on its parsed TOML.
fn edit_config(home: &Path, edit: impl FnOnce(&mut toml::Value)) {
    let path = home.join("config.toml");
    let mut config: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    edit(&mut config);
    fs::write(path, toml::to_string(&config).unwrap()).unwrap();
}

/// Current policy is enforced on resume without replacing the frozen
/// execution: a changed native model alias, a role that now requires a new
/// constraint, and a full current harness cap each refuse with a policy (or
/// capacity) error — never the missing-proof refusal — and admit nothing.
#[tokio::test]
async fn provider_resume_enforces_current_alias_role_and_cap() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let id = completed_parent(&home, &service, "policy-parent").await;
    let parent = || Store::open(&home).unwrap().get(&id).unwrap();
    let resume = |request_id: &str| {
        service
            .admit_provider_resume(
                &parent(),
                "fixture:answer".into(),
                None,
                Some(request_id.into()),
                None,
            )
            .map_err(|error| error.to_string())
    };
    let original = fs::read_to_string(home.join("config.toml")).unwrap();
    edit_config(&home, |config| {
        config["providers"]["glm-user"]["models"][0]
            .as_table_mut()
            .unwrap()
            .insert("native_model".into(), "other-native".into());
    });
    let alias = resume("alias").unwrap_err();
    assert!(alias.contains("native model alias"), "{alias}");
    fs::write(home.join("config.toml"), &original).unwrap();

    let profile = home.join("profiles/review.md");
    let role = fs::read_to_string(&profile).unwrap();
    fs::write(
        &profile,
        role.replace(
            "required_constraints = []",
            "required_constraints = [\"filesystem_read_isolation\"]",
        ),
    )
    .unwrap();
    let grants = resume("grants").unwrap_err();
    assert!(
        grants.contains("role grants") || grants.contains("harness policy"),
        "{grants}"
    );
    fs::write(&profile, &role).unwrap();

    edit_config(&home, |config| {
        config["harnesses"]["claude-code"]
            .as_table_mut()
            .unwrap()
            .insert("max_active_agents".into(), 1.into());
    });
    let mut busy = request(&home);
    busy.request_id = Some("occupies-the-cap".into());
    let revision = Store::open(&home)
        .unwrap()
        .quota_capacity_revision()
        .unwrap();
    service
        .admit_provider_trusted(busy, candidates(revision))
        .unwrap();
    let capped = resume("capped").unwrap_err();
    assert!(capped.to_lowercase().contains("capacity"), "{capped}");
    let children: i64 = Store::open(&home)
        .unwrap()
        .conn
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE parent_agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(children, 0);
}

/// A resumed child cancelled between admission and spawn never starts a
/// process, releases its own attempt with never-spawned proof, and leaves
/// the sealed native history byte-identical.
#[tokio::test]
async fn cancelled_resume_never_spawns_or_touches_history() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let id = completed_parent(&home, &service, "cancel-parent").await;
    let seal = attempt_state(&home, &id)["native_history"]["seal"].clone();
    let history = std::path::Path::new(seal["root"].as_str().unwrap())
        .join(seal["relative"].as_str().unwrap());
    let before = fs::read(&history).unwrap();
    let child = service
        .admit_provider_resume(
            &Store::open(&home).unwrap().get(&id).unwrap(),
            "fixture:answer".into(),
            None,
            Some("cancelled-child".into()),
            None,
        )
        .unwrap();
    let child: AgentId = serde_json::from_value(child["agent_id"].clone()).unwrap();
    service.cancel(&child).unwrap();
    run_to_end(&home, &child).await;
    let row = Store::open(&home).unwrap().get(&child).unwrap();
    assert_eq!(row.status, Status::Cancelled);
    let (process, active, proof): (Option<String>, i64, String) = Store::open(&home)
        .unwrap()
        .conn
        .query_row(
            "SELECT process_identity,ownership_active,cleanup_proof_json FROM attempts WHERE agent_id=?",
            [child.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert!(process.is_none());
    assert_eq!(active, 0);
    assert!(proof.contains("never_spawned"), "{proof}");
    assert_eq!(fs::read(&history).unwrap(), before);
}

/// One selector case: name, candidate intent, (account, rank) candidates,
/// pinned account, and the expected selection (`None` = refused).
type SelectorCase<'a> = (
    &'a str,
    SelectionIntent,
    Vec<(&'a AccountId, u32)>,
    Option<&'a AccountId>,
    Option<&'a AccountId>,
);

/// The store selector for a resume keeps the parent's account while it is a
/// valid candidate even when another ranks better, moves only when it is no
/// longer a candidate, and a pinned resume never moves.
#[tokio::test]
async fn resume_selector_keeps_the_parent_account_until_it_is_unavailable() {
    use agent_run_store::provider_admission::ProviderResume;
    let extra = "[[providers.glm-user.bindings]]\nlabel = \"alt\"\naccount = \"acct-alt\"\n";
    let (_temp, home) = home_with(extra, &["acct-alt"]);
    let service = Service::new(home.clone());
    let work: AccountId = "acct-work".parse().unwrap();
    let alt: AccountId = "acct-alt".parse().unwrap();
    let set = |intent: SelectionIntent, accounts: &[(&AccountId, u32)]| QuotaCandidateSet {
        provider: "glm-user".parse().unwrap(),
        model: "fixture".into(),
        intent,
        candidates: accounts
            .iter()
            .map(|(account, rank)| QuotaCandidate {
                account: (*account).clone(),
                rank: *rank,
                physical_keys: vec![
                    agent_run_domain::PhysicalQuotaKey::new(account, "tokens").unwrap()
                ],
                multiplier: PositiveFinite::try_from(1.0).unwrap(),
                quota_known: true,
            })
            .collect(),
        capacity_revision: Store::open(&home)
            .unwrap()
            .quota_capacity_revision()
            .unwrap(),
    };
    // Each case needs its own terminal parent (one child per parent).
    let cases: [SelectorCase<'_>; 3] = [
        (
            "keep",
            SelectionIntent::Auto,
            vec![(&alt, 0), (&work, 1)],
            None,
            Some(&work),
        ),
        (
            "move",
            SelectionIntent::Auto,
            vec![(&alt, 0)],
            None,
            Some(&alt),
        ),
        (
            "pinned",
            SelectionIntent::Pinned(work.clone()),
            vec![(&alt, 0)],
            Some(&work),
            None,
        ),
    ];
    for (name, intent, accounts, pinned, expected) in cases {
        let id = completed_parent(&home, &service, &format!("{name}-parent")).await;
        let parent = Store::open(&home).unwrap().get(&id).unwrap();
        let mut identity = parent.identity.clone().unwrap();
        let mut request: ProviderStartRequest =
            serde_json::from_value(identity["provider_request"].clone()).unwrap();
        request.task = "fixture:answer".into();
        request.request_id = Some(format!("{name}-child"));
        identity["provider_request"] = serde_json::to_value(&request).unwrap();
        identity["replay_request_sha256"] =
            agent_run_domain::canonical::sha256_hex(&serde_json::to_value(&request).unwrap(), true)
                .into();
        identity["authority"]["eligible_accounts"] = serde_json::json!(["acct-alt", "acct-work"]);
        let authority: agent_run_domain::catalog::ResolvedLaunchAuthority =
            serde_json::from_value(identity["authority"].clone()).unwrap();
        let mut effective = parent.request.clone();
        effective.task = request.task.clone();
        effective.request_id = request.request_id.clone();
        let config: agent_run_config::provider_config::ProviderConfig =
            serde_json::from_value(identity["provider_config"].clone()).unwrap();
        let catalog = config
            .resolve_catalog(Store::open(&home).unwrap().list_accounts().unwrap())
            .unwrap();
        let result = Store::open(&home).unwrap().admit_provider_resume(
            &request,
            &effective,
            &catalog,
            &authority,
            &set(intent, &accounts),
            &identity,
            8,
            None,
            pinned,
            ProviderResume {
                parent: &id,
                prefer: &work,
            },
        );
        match expected {
            Some(account) => assert_eq!(&result.unwrap().account_id, account, "{name}"),
            None => assert!(result.is_err(), "{name}: a pinned resume never switches"),
        }
    }
}

/// The handoff binds the parent's seal to the history root the child's
/// launch plan actually selects: a seal recorded for another (equally valid)
/// root passes admission but is refused before spawn, with no process and
/// both histories byte-identical.
#[tokio::test]
async fn handoff_refuses_a_seal_for_another_history_root() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let id = completed_parent(&home, &service, "root-parent").await;
    let mut state = attempt_state(&home, &id);
    let seal = state["native_history"]["seal"].clone();
    let planned = std::path::PathBuf::from(seal["root"].as_str().unwrap());
    let relative = seal["relative"].as_str().unwrap().to_owned();
    let other = home.join("other-claude-config");
    fs::create_dir_all(other.join(&relative).parent().unwrap()).unwrap();
    fs::copy(planned.join(&relative), other.join(&relative)).unwrap();
    let other = other.canonicalize().unwrap();
    state["native_history"]["seal"]["root"] = other.to_string_lossy().into_owned().into();
    Store::open(&home)
        .unwrap()
        .conn
        .execute(
            "UPDATE attempts SET adapter_state_json=? WHERE agent_id=?",
            rusqlite::params![state.to_string(), id.as_str()],
        )
        .unwrap();
    let before = (
        fs::read(planned.join(&relative)).unwrap(),
        fs::read(other.join(&relative)).unwrap(),
    );
    let child = service
        .admit_provider_resume(
            &Store::open(&home).unwrap().get(&id).unwrap(),
            "fixture:answer".into(),
            None,
            Some("root-child".into()),
            None,
        )
        .unwrap();
    let child: AgentId = serde_json::from_value(child["agent_id"].clone()).unwrap();
    let mut supervisor = supervisor(&home, &child);
    let _ = tokio::time::timeout(Duration::from_secs(20), supervisor.wait())
        .await
        .unwrap();
    let row = Store::open(&home).unwrap().get(&child).unwrap();
    assert!(
        row.status.terminal() && row.status != Status::Succeeded,
        "{:?}",
        row.status
    );
    assert!(
        row.failure_text
            .as_deref()
            .unwrap_or_default()
            .contains("another storage root"),
        "{:?}",
        row.failure_text
    );
    let process: Option<String> = Store::open(&home)
        .unwrap()
        .conn
        .query_row(
            "SELECT process_identity FROM attempts WHERE agent_id=?",
            [child.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(process.is_none(), "no process was spawned");
    assert_eq!(
        (
            fs::read(planned.join(&relative)).unwrap(),
            fs::read(other.join(&relative)).unwrap()
        ),
        before
    );
}

/// Every record a provider supervisor journals — phases, transcript, runtime
/// session, cleanup, history evidence and the terminal events — carries its
/// attempt id, including those written after ownership was released.
#[tokio::test]
async fn supervisor_records_are_bound_to_their_attempt() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let id = completed_parent(&home, &service, "bound-parent").await;
    let store = Store::open(&home).unwrap();
    let attempt: String = store
        .conn
        .query_row(
            "SELECT id FROM attempts WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let unbound: Vec<String> = store
        .conn
        .prepare(
            "SELECT kind FROM events WHERE agent_id=? AND (attempt_id IS NULL OR attempt_id!=?)",
        )
        .unwrap()
        .query_map(rusqlite::params![id.as_str(), attempt], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert!(unbound.is_empty(), "{unbound:?}");
    let messages: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE agent_id=? AND (attempt_id IS NULL OR attempt_id!=?)",
            rusqlite::params![id.as_str(), attempt],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(messages, 0);
}

/// The fake engine's authoritative `rate_limit_event` rejection crosses the
/// adapter/supervisor boundary as a typed `native_failure` event bound to the
/// attempt, with account/provider/model from the supervisor; the same JSON
/// quoted in assistant text produces nothing.
#[tokio::test]
async fn quota_signals_cross_the_boundary_typed_and_attempt_bound() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let run = |task: &'static str, request_id: &'static str| {
        let mut request = request(&home);
        request.task = task.into();
        request.request_id = Some(request_id.into());
        let revision = Store::open(&home)
            .unwrap()
            .quota_capacity_revision()
            .unwrap();
        let admitted = service
            .admit_provider_trusted(request, candidates(revision))
            .unwrap();
        serde_json::from_value::<AgentId>(admitted["agent_id"].clone()).unwrap()
    };
    let quota = run("fixture:quota", "quota-1");
    run_to_end(&home, &quota).await;
    let quoted = run("fixture:quota-text", "quota-2");
    run_to_end(&home, &quoted).await;
    let store = Store::open(&home).unwrap();
    let failures = |id: &AgentId| -> Vec<(Option<String>, String)> {
        store
            .conn
            .prepare("SELECT attempt_id,data_json FROM events WHERE agent_id=? AND kind='native_failure'")
            .unwrap()
            .query_map([id.as_str()], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    let recorded = failures(&quota);
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    let attempt: String = store
        .conn
        .query_row(
            "SELECT id FROM attempts WHERE agent_id=?",
            [quota.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(recorded[0].0.as_deref(), Some(attempt.as_str()));
    let data: serde_json::Value = serde_json::from_str(&recorded[0].1).unwrap();
    assert_eq!(data["class"], "quota_exhausted");
    assert_eq!(data["signal"], "claude.rate_limit_event.rejected");
    assert_eq!(data["window"], "five_hour");
    assert_eq!(data["account"], "acct-work");
    assert_eq!(data["provider"], "glm-user");
    assert_eq!(data["attempt"], attempt.as_str());
    assert_eq!(store.get(&quota).unwrap().status, Status::Failed);
    assert!(failures(&quoted).is_empty(), "quoted text is not a signal");
    assert_eq!(store.get(&quoted).unwrap().status, Status::Succeeded);
}

/// A rejection that a later protocol state supersedes is not the terminal
/// cause: an `allowed` event, or an assistant `authentication_failed` error
/// after the rejection, ends the failed turn without a quota disposition.
#[tokio::test]
async fn superseded_quota_rejections_do_not_classify_the_failure() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    for (task, request_id, expected) in [
        ("fixture:quota-then-allowed", "sup-1", None),
        ("fixture:quota-then-auth", "sup-2", Some("auth")),
        ("fixture:quota", "sup-3", Some("quota_exhausted")),
    ] {
        let mut request = request(&home);
        request.task = task.into();
        request.request_id = Some(request_id.into());
        let revision = Store::open(&home)
            .unwrap()
            .quota_capacity_revision()
            .unwrap();
        let admitted = service
            .admit_provider_trusted(request, candidates(revision))
            .unwrap();
        let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
        run_to_end(&home, &id).await;
        let store = Store::open(&home).unwrap();
        assert_eq!(store.get(&id).unwrap().status, Status::Failed, "{task}");
        let class: Option<String> = store
            .conn
            .query_row(
                "SELECT json_extract(data_json,'$.class') FROM events WHERE agent_id=? AND kind='native_failure'",
                [id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(class.as_deref(), expected, "{task}");
    }
}

/// A home with a native Codex provider `codex-user` (model `fixture`, served
/// by the fixture app-server) bound to accounts `acct-cx-a`/`acct-cx-b`, whose
/// linked auth files carry `auth` contents (`exhausted` makes its turns fail
/// with `usageLimitExceeded`). Dummy files only; no real credential.
fn codex_home(auth: [&str; 2]) -> (tempfile::TempDir, std::path::PathBuf) {
    let extra = "[providers.codex-user]\nharness = \"codex\"\nconnection = { kind = \"native\" }\nauth_family = \"openai\"\nlimits_source = \"none\"\n[[providers.codex-user.models]]\nid = \"fixture\"\n[[providers.codex-user.bindings]]\nlabel = \"a\"\naccount = \"acct-cx-a\"\n[[providers.codex-user.bindings]]\nlabel = \"b\"\naccount = \"acct-cx-b\"\n";
    let (temp, home) = home_with(extra, &[]);
    let mut store = Store::open(&home).unwrap();
    for ((label, account), content) in [("a", "acct-cx-a"), ("b", "acct-cx-b")]
        .into_iter()
        .zip(auth)
    {
        let dir = home.join("accounts/codex").join(label);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("auth.json"), content).unwrap();
        store
            .register_account(&AccountRecord {
                account_id: account.parse().unwrap(),
                auth_family: "openai".parse().unwrap(),
                secret_ref: format!("named:codex:{label}").parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
    }
    (temp, home)
}

/// Admits one ranked `codex-user` run (optionally pinned) and runs its
/// supervisor to the end.
async fn codex_run(home: &Path, request_id: &str, account: Option<&str>) -> AgentId {
    let mut request: ProviderStartRequest = serde_json::from_value(serde_json::json!({
        "provider":"codex-user","model":"fixture","profile":"review",
        "task":"fixture:original-task","workdir":home,"request_id":request_id,"account":account,
        "orchestrator":{"transport":"fixture","external_session_id":"codex-session"},
    }))
    .unwrap();
    request.validate().unwrap();
    let admitted = Service::new(home.to_path_buf())
        .admit_provider(request)
        .unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    run_to_end(home, &id).await;
    id
}

/// (number, account, state, ownership, finished) of every attempt, in order.
fn attempts(home: &Path, id: &AgentId) -> Vec<(u32, String, String, i64, bool)> {
    Store::open(home)
        .unwrap()
        .conn
        .prepare("SELECT number,selected_account_id,state,ownership_active,finished_at IS NOT NULL FROM attempts WHERE agent_id=? ORDER BY number")
        .unwrap()
        .query_map([id.as_str()], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

/// One count query for `id`.
fn count(home: &Path, sql: &str, id: &AgentId) -> i64 {
    Store::open(home)
        .unwrap()
        .conn
        .query_row(sql, [id.as_str()], |row| row.get(0))
        .unwrap()
}

/// Automatic in-flight switch: A's authoritative usage-limit failure closes
/// A, allocates B on the same logical agent, continues the same native thread
/// with one internal control turn (the original task is sent and journaled
/// once), and yields exactly one success, answer and delivery.
#[tokio::test]
async fn exhausted_account_switches_within_the_same_logical_run() {
    let (_temp, home) = codex_home(["exhausted", "ok"]);
    let id = codex_run(&home, "switch-1", None).await;
    let store = Store::open(&home).unwrap();
    let row = store.get(&id).unwrap();
    assert_eq!(
        row.status,
        Status::Succeeded,
        "{:?} {:?}",
        row.failure_kind,
        row.failure_text
    );
    let thread = row.runtime_session_id.clone().unwrap();
    let attempts = attempts(&home, &id);
    assert_eq!(attempts.len(), 2, "{attempts:?}");
    assert_eq!(
        (
            attempts[0].1.as_str(),
            attempts[0].2.as_str(),
            attempts[0].3,
            attempts[0].4
        ),
        ("acct-cx-a", "exhausted", 0, true)
    );
    assert_eq!((attempts[1].1.as_str(), attempts[1].3), ("acct-cx-b", 0));
    assert_eq!(
        Service::new(home.clone()).answer(&id).unwrap()["content"],
        format!("fixture codex answer on {thread}")
    );
    // The original task is journaled once; the switch sends only the control.
    assert_eq!(
        count(
            &home,
            "SELECT COUNT(*) FROM messages WHERE agent_id=? AND role='user'",
            &id
        ),
        1
    );
    let rollout = fs::read_to_string(
        std::path::Path::new(
            row.identity.as_ref().unwrap()["runtime_home"]
                .as_str()
                .unwrap(),
        )
        .join(format!(
            "sessions/2026/09/23/rollout-fixture-{thread}.jsonl"
        )),
    )
    .unwrap();
    let inputs: Vec<String> = rollout
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|record| record["payload"]["role"] == "user")
        .map(|record| record["payload"]["content"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(inputs.len(), 2, "{inputs:?}");
    assert!(inputs[0].contains("fixture:original-task"));
    assert!(!inputs[1].contains("fixture:original-task"));
    assert!(inputs[1].contains(agent_run::supervisor::CONTINUATION_CONTROL));
    // Attempt-bound evidence and exactly one delivery.
    let bound = |kind: &str| -> Option<u32> {
        store
            .conn
            .query_row(
                "SELECT t.number FROM events e JOIN attempts t ON t.id=e.attempt_id WHERE e.agent_id=? AND e.kind=?",
                rusqlite::params![id.as_str(), kind],
                |row| row.get(0),
            )
            .optional()
            .unwrap()
    };
    assert_eq!(bound("native_failure"), Some(1));
    assert_eq!(bound("continuation_control"), Some(2));
    assert_eq!(
        count(
            &home,
            "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
            &id
        ),
        1
    );
    assert_eq!(
        count(
            &home,
            "SELECT COUNT(*) FROM agents WHERE root_agent_id=?",
            &id
        ),
        1
    );
}

/// Bounded stops: both accounts exhausted, a pinned run, and the only other
/// account disabled now each end the one logical run failed with
/// `quota_exhausted` and the exact blocker, with one delivery and no further
/// attempt than the proven ones.
#[tokio::test]
async fn exhausted_runs_stop_with_the_exact_blocker() {
    for (auth, pin, disable, blocker, tried) in [
        (
            ["exhausted", "exhausted"],
            None,
            None,
            "no_eligible_account",
            2,
        ),
        (["exhausted", "ok"], Some("a"), None, "pinned_account", 1),
        (
            ["exhausted", "ok"],
            None,
            Some("acct-cx-b"),
            "no_eligible_account",
            1,
        ),
    ] {
        let (_temp, home) = codex_home(auth);
        if let Some(account) = disable {
            Store::open(&home)
                .unwrap()
                .disable_account(&account.parse().unwrap())
                .unwrap();
        }
        let id = codex_run(&home, "stop-1", pin).await;
        let row = Store::open(&home).unwrap().get(&id).unwrap();
        assert_eq!(row.status, Status::Failed, "{blocker}");
        assert_eq!(
            row.failure_kind.as_deref(),
            Some("quota_exhausted"),
            "{blocker}"
        );
        assert!(
            row.failure_text
                .as_deref()
                .unwrap_or_default()
                .contains(blocker),
            "{:?}",
            row.failure_text
        );
        let attempts = attempts(&home, &id);
        assert_eq!(attempts.len(), tried, "{blocker}: {attempts:?}");
        assert!(
            attempts.iter().all(|attempt| attempt.3 == 0),
            "no owned attempt remains"
        );
        assert_eq!(
            count(
                &home,
                "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
                &id
            ),
            1
        );
        assert_eq!(
            count(
                &home,
                "SELECT COUNT(*) FROM messages WHERE agent_id=? AND role='user'",
                &id
            ),
            1
        );
    }
}

/// A cancel that arrives while the exhausted attempt is finishing wins: no
/// next attempt is allocated and the run ends cancelled with one delivery.
#[tokio::test]
async fn cancel_during_the_exhausted_attempt_prevents_the_switch() {
    let (_temp, home) = codex_home(["exhausted-hold", "ok"]);
    let mut request: ProviderStartRequest = serde_json::from_value(serde_json::json!({
        "provider":"codex-user","model":"fixture","profile":"review",
        "task":"fixture:original-task","workdir":home,"request_id":"cancel-switch",
        "orchestrator":{"transport":"fixture","external_session_id":"codex-session"},
    }))
    .unwrap();
    request.validate().unwrap();
    let service = Service::new(home.clone());
    let admitted = service.admit_provider(request).unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    let mut child = supervisor(&home, &id);
    let runtime_home = || {
        Store::open(&home)
            .unwrap()
            .get(&id)
            .unwrap()
            .identity
            .unwrap()["runtime_home"]
            .as_str()
            .map(std::path::PathBuf::from)
    };
    let mut held = None;
    for _ in 0..400 {
        if let Some(root) = runtime_home().filter(|root| root.join("fixture-held").exists()) {
            held = Some(root);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let root = held.expect("attempt A reached the barrier");
    service.cancel(&id).unwrap();
    fs::write(root.join("fixture-release"), "").unwrap();
    tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .unwrap()
        .unwrap();
    let row = Store::open(&home).unwrap().get(&id).unwrap();
    assert_eq!(row.status, Status::Cancelled, "{:?}", row.failure_text);
    assert_eq!(
        attempts(&home, &id).len(),
        1,
        "no next attempt after cancel"
    );
    assert_eq!(
        count(
            &home,
            "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
            &id
        ),
        1
    );
}

/// Claude Code keeps history per login: an exhausted Claude-harness run with
/// a second bound account still stops with the cross-account blocker.
#[tokio::test]
async fn claude_exhaustion_never_switches_accounts() {
    let extra = "[[providers.glm-user.bindings]]\nlabel = \"alt\"\naccount = \"acct-alt\"\n";
    let (_temp, home) = home_with(extra, &["acct-alt"]);
    let service = Service::new(home.clone());
    let mut request = request(&home);
    request.task = "fixture:quota".into();
    request.account = None;
    request.request_id = Some("claude-stop".into());
    let revision = Store::open(&home)
        .unwrap()
        .quota_capacity_revision()
        .unwrap();
    let mut set = candidates(revision);
    set.intent = SelectionIntent::Auto;
    let admitted = service.admit_provider_trusted(request, set).unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    run_to_end(&home, &id).await;
    let row = Store::open(&home).unwrap().get(&id).unwrap();
    assert_eq!(row.failure_kind.as_deref(), Some("quota_exhausted"));
    assert!(row
        .failure_text
        .as_deref()
        .unwrap_or_default()
        .contains("cross_account_continuation_unverified"));
    assert_eq!(attempts(&home, &id).len(), 1);
}

/// A supervisor killed while the switched attempt B is running leaves one
/// owned attempt and a live native process. Reconciliation ends the one
/// logical run without success and allocates nothing further; its periodic
/// orphan pass then re-adopts B's recorded leader (same token and birth),
/// terminates the verified group and releases B only on confirmed cleanup.
/// A second periodic pass changes nothing. The closed attempt A keeps its
/// `exhausted` state and there is at most one delivery.
#[tokio::test]
async fn crash_after_the_switch_reconciles_without_a_duplicate() {
    let (_temp, home) = codex_home(["exhausted", "ok-hold"]);
    let mut request: ProviderStartRequest = serde_json::from_value(serde_json::json!({
        "provider":"codex-user","model":"fixture","profile":"review",
        "task":"fixture:original-task","workdir":home,"request_id":"crash-switch",
        "orchestrator":{"transport":"fixture","external_session_id":"codex-session"},
    }))
    .unwrap();
    request.validate().unwrap();
    let service = Service::new(home.clone());
    let admitted = service.admit_provider(request).unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    let mut child = supervisor(&home, &id);
    let mut held = None;
    for _ in 0..400 {
        let root = Store::open(&home)
            .unwrap()
            .get(&id)
            .unwrap()
            .identity
            .unwrap()["runtime_home"]
            .as_str()
            .map(std::path::PathBuf::from);
        if let Some(root) = root.filter(|root| root.join("fixture-held").exists()) {
            held = Some(root);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let root = held.expect("attempt B reached the barrier");
    child.kill().await.unwrap();
    let _ = child.wait().await;
    assert_eq!(attempts(&home, &id).len(), 2);
    let reconciled = service.reconcile().unwrap();
    assert!(reconciled >= 1, "the orphaned run was reconciled");
    let pid = Store::open(&home)
        .unwrap()
        .get(&id)
        .unwrap()
        .process_group_id
        .unwrap();
    assert_eq!(
        service.reconcile().unwrap(),
        0,
        "a second periodic pass is a no-op"
    );
    fs::write(root.join("fixture-release"), "").unwrap();
    let row = Store::open(&home).unwrap().get(&id).unwrap();
    assert!(
        row.status.terminal() && row.status != Status::Succeeded,
        "{:?}",
        row.status
    );
    let attempts = attempts(&home, &id);
    assert_eq!(
        attempts.len(),
        2,
        "no attempt after the crash: {attempts:?}"
    );
    assert_eq!(
        (attempts[0].2.as_str(), attempts[0].3),
        ("exhausted", 0),
        "{attempts:?}"
    );
    assert_eq!(
        (attempts[1].2.as_str(), attempts[1].3),
        ("lost", 0),
        "{attempts:?}"
    );
    let proof: String = Store::open(&home)
        .unwrap()
        .conn
        .query_row(
            "SELECT cleanup_proof_json FROM attempts WHERE agent_id=? AND number=2",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    let proof: serde_json::Value = serde_json::from_str(&proof).unwrap();
    assert_eq!(proof["confirmed"], true, "{proof}");
    assert_eq!(proof["reconciled"], true, "{proof}");
    // SAFETY: signal 0 only probes the recorded (now expected gone) group.
    let probe = unsafe { libc::kill(-pid, 0) };
    assert_eq!(probe, -1, "B's process group is gone");
    let event_attempt: u32 = Store::open(&home)
        .unwrap()
        .conn
        .query_row(
            "SELECT t.number FROM events e JOIN attempts t ON t.id=e.attempt_id WHERE e.agent_id=? AND e.kind='attempt_cleanup_reconciled'",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(event_attempt, 2);
    assert!(
        count(
            &home,
            "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
            &id
        ) <= 1
    );
}

/// Runs a `codex-user` run whose attempt A holds at the fixture barrier
/// before its exhausted terminal frame, applies `during` (given the run's
/// native home) while A is held, then releases A and waits for the end.
async fn held_codex_run(home: &Path, during: impl FnOnce(&Path)) -> AgentId {
    let mut request: ProviderStartRequest = serde_json::from_value(serde_json::json!({
        "provider":"codex-user","model":"fixture","profile":"review",
        "task":"fixture:original-task","workdir":home,"request_id":"held-run",
        "orchestrator":{"transport":"fixture","external_session_id":"codex-session"},
    }))
    .unwrap();
    request.validate().unwrap();
    let admitted = Service::new(home.to_path_buf())
        .admit_provider(request)
        .unwrap();
    let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
    let mut child = supervisor(home, &id);
    let mut held = None;
    for _ in 0..400 {
        let root = Store::open(home)
            .unwrap()
            .get(&id)
            .unwrap()
            .identity
            .unwrap()["runtime_home"]
            .as_str()
            .map(std::path::PathBuf::from);
        if let Some(root) = root.filter(|root| root.join("fixture-held").exists()) {
            held = Some(root);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let root = held.expect("attempt A reached the barrier");
    during(&root);
    fs::write(root.join("fixture-release"), "").unwrap();
    tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .unwrap()
        .unwrap();
    id
}

/// The switch re-reads current policy and continuation evidence: a model
/// restriction added while A ran, or a tool call left pending in A's native
/// history, stops the run with the exact blocker and no attempt B.
#[tokio::test]
async fn switch_honors_current_policy_and_history_evidence() {
    let (_temp, home) = codex_home(["exhausted-hold", "ok"]);
    let policy_home = home.clone();
    let id = held_codex_run(&home, move |_| {
        edit_config(&policy_home, |config| {
            let models = config["providers"]["codex-user"]["models"]
                .as_array_mut()
                .unwrap();
            models[0].as_table_mut().unwrap().insert(
                "restrictions".into(),
                toml::Value::Array(vec!["filesystem_read_isolation".into()]),
            );
        });
    })
    .await;
    let row = Store::open(&home).unwrap().get(&id).unwrap();
    assert!(
        row.failure_text
            .as_deref()
            .unwrap_or_default()
            .contains("current_policy_refused"),
        "{:?}",
        row.failure_text
    );
    assert_eq!(attempts(&home, &id).len(), 1);

    let (_temp, home) = codex_home(["exhausted-hold", "ok"]);
    let id = held_codex_run(&home, |root| {
        let rollout = fs::read_dir(root.join("sessions/2026/09/23"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mut file = fs::OpenOptions::new().append(true).open(rollout).unwrap();
        std::io::Write::write_all(
            &mut file,
            b"{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"pending\"}}\n",
        )
        .unwrap();
    })
    .await;
    let row = Store::open(&home).unwrap().get(&id).unwrap();
    assert_eq!(row.failure_kind.as_deref(), Some("quota_exhausted"));
    assert!(
        row.failure_text
            .as_deref()
            .unwrap_or_default()
            .contains("continuation_unavailable"),
        "{:?}",
        row.failure_text
    );
    assert_eq!(attempts(&home, &id).len(), 1);
}

/// A terminal run that still owns a `prepared` attempt with no recorded
/// child (a crash between allocation and spawn) is closed by reconciliation
/// with exact never-spawned evidence, and nothing else changes.
#[tokio::test]
async fn reconcile_closes_a_never_spawned_owned_attempt() {
    let (_temp, home) = home();
    let service = Service::new(home.clone());
    let id = completed_parent(&home, &service, "never-parent").await;
    let store = Store::open(&home).unwrap();
    store
        .conn
        .execute(
            "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,selected_account_id,phase,ownership_active) \
             VALUES('att_orphan',?,2,'prepared','{}',0,'acct-work','prepared',1)",
            [id.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute("UPDATE agents SET status='lost' WHERE id=?", [id.as_str()])
        .unwrap();
    service.reconcile().unwrap();
    let (owned, proof): (i64, String) = Store::open(&home)
        .unwrap()
        .conn
        .query_row(
            "SELECT ownership_active,cleanup_proof_json FROM attempts WHERE id='att_orphan'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(owned, 0);
    assert!(proof.contains("never_spawned"), "{proof}");
    assert_eq!(attempts(&home, &id).len(), 2);
}

/// Admits one automatic `codex-user` run with an optional timeout, without
/// starting its supervisor.
fn codex_admit(home: &Path, request_id: &str, timeout: Option<f64>) -> AgentId {
    let mut request: ProviderStartRequest = serde_json::from_value(serde_json::json!({
        "provider":"codex-user","model":"fixture","profile":"review",
        "task":"fixture:original-task","workdir":home,"request_id":request_id,
        "timeout_seconds":timeout,
        "orchestrator":{"transport":"fixture","external_session_id":"codex-session"},
    }))
    .unwrap();
    request.validate().unwrap();
    let admitted = Service::new(home.to_path_buf())
        .admit_provider(request)
        .unwrap();
    serde_json::from_value(admitted["agent_id"].clone()).unwrap()
}

/// (created_at, finished_at) of the agent and of attempt `number`.
fn times(home: &Path, id: &AgentId, number: u32) -> ((f64, f64), (f64, f64)) {
    let store = Store::open(home).unwrap();
    let agent = store
        .conn
        .query_row(
            "SELECT created_at,finished_at FROM agents WHERE id=?",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let attempt = store
        .conn
        .query_row(
            "SELECT created_at,finished_at FROM attempts WHERE agent_id=? AND number=?",
            rusqlite::params![id.as_str(), number],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    (agent, attempt)
}

/// One deadline spans the whole run: A (a 1.5 s exhausted turn) consumes part
/// of a 5 s budget, B hangs and is stopped at the ORIGINAL deadline — not 5 s
/// after B started (a fresh budget would end at least ~7 s after admission) — with confirmed cleanup, `timed_out` once, and no third
/// attempt.
#[tokio::test]
async fn one_deadline_spans_attempts_and_cleans_a_hung_engine() {
    let (_temp, home) = codex_home(["exhausted-slow", "ok-hold"]);
    let id = codex_admit(&home, "deadline-1", Some(5.0));
    run_to_end(&home, &id).await;
    let row = Store::open(&home).unwrap().get(&id).unwrap();
    assert_eq!(row.status, Status::TimedOut, "{:?}", row.failure_text);
    let attempts = attempts(&home, &id);
    assert_eq!(attempts.len(), 2, "{attempts:?}");
    assert!(
        attempts.iter().all(|attempt| attempt.3 == 0),
        "{attempts:?}"
    );
    let ((created, finished), (b_created, b_finished)) = times(&home, &id, 2);
    assert!(b_created - created >= 1.4, "A consumed part of the budget");
    assert!(
        finished - created < 6.3,
        "B only had the remainder: {}",
        finished - created
    );
    assert!(
        b_finished - b_created < 5.0 - 1.4,
        "B's own runtime was the remainder"
    );
    let proof: String = Store::open(&home)
        .unwrap()
        .conn
        .query_row(
            "SELECT cleanup_proof_json FROM attempts WHERE agent_id=? AND number=2",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(proof.contains("\"confirmed\":true"), "{proof}");
    assert_eq!(
        count(
            &home,
            "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
            &id
        ),
        1
    );
}

/// A deadline that expires while A finishes (positioned by shortening the
/// durable timeout during A) allocates and spawns no B.
#[tokio::test]
async fn expiry_between_attempts_spawns_no_next_attempt() {
    let (_temp, home) = codex_home(["exhausted-hold", "ok"]);
    let shorten = home.clone();
    let id = held_codex_run(&home, move |_| {
        Store::open(&shorten)
            .unwrap()
            .conn
            .execute("UPDATE agents SET timeout_seconds=0.001", [])
            .unwrap();
    })
    .await;
    let row = Store::open(&home).unwrap().get(&id).unwrap();
    assert_eq!(row.status, Status::TimedOut, "{:?}", row.failure_text);
    assert_eq!(attempts(&home, &id).len(), 1);
}

/// Handoff barrier after B was allocated and planned but before it spawned:
/// a cancel accepted there, a revoked account, or a newly refused policy each
/// stop B from spawning (never-spawned evidence, no process), with the typed
/// outcome and one delivery.
#[tokio::test]
async fn handoff_after_allocation_honours_cancel_and_revocation() {
    for case in ["cancel", "revoke", "policy"] {
        let (_temp, home) = codex_home(["exhausted", "ok"]);
        let id = codex_admit(&home, "handoff-1", None);
        fs::write(home.join("fixture-pause-spawn-2"), "").unwrap();
        let mut child = supervisor(&home, &id);
        for _ in 0..400 {
            if home.join("fixture-paused-spawn-2").exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            home.join("fixture-paused-spawn-2").exists(),
            "{case}: B paused"
        );
        match case {
            "cancel" => {
                Service::new(home.clone()).cancel(&id).unwrap();
            }
            "revoke" => Store::open(&home)
                .unwrap()
                .disable_account(&"acct-cx-b".parse().unwrap())
                .unwrap(),
            _ => edit_config(&home, |config| {
                config["providers"]["codex-user"]["models"][0]
                    .as_table_mut()
                    .unwrap()
                    .insert(
                        "restrictions".into(),
                        toml::Value::Array(vec!["filesystem_read_isolation".into()]),
                    );
            }),
        }
        fs::write(home.join("fixture-resume-spawn-2"), "").unwrap();
        tokio::time::timeout(Duration::from_secs(20), child.wait())
            .await
            .unwrap()
            .unwrap();
        let row = Store::open(&home).unwrap().get(&id).unwrap();
        let (process, proof, owned): (Option<String>, String, i64) = Store::open(&home)
            .unwrap()
            .conn
            .query_row(
                "SELECT process_identity,cleanup_proof_json,ownership_active FROM attempts WHERE agent_id=? AND number=2",
                [id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert!(process.is_none(), "{case}: B never spawned");
        assert!(proof.contains("never_spawned"), "{case}: {proof}");
        assert_eq!(owned, 0, "{case}");
        match case {
            "cancel" => assert_eq!(row.status, Status::Cancelled),
            "revoke" => assert!(
                row.failure_text
                    .as_deref()
                    .unwrap_or_default()
                    .contains("account_revoked_at_handoff"),
                "{:?}",
                row.failure_text
            ),
            _ => assert!(
                row.failure_text
                    .as_deref()
                    .unwrap_or_default()
                    .contains("current_policy_refused"),
                "{:?}",
                row.failure_text
            ),
        }
        assert_eq!(
            count(
                &home,
                "SELECT COUNT(*) FROM deliveries WHERE agent_id=?",
                &id
            ),
            1,
            "{case}"
        );
        assert_eq!(
            count(
                &home,
                "SELECT COUNT(*) FROM messages WHERE agent_id=? AND role='user'",
                &id
            ),
            1,
            "{case}"
        );
    }
}
