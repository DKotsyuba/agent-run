//! Synthetic Quota-v2 admission tests using the real client and writer.
mod common;

use agent_run_core::service::Service;
use agent_run_domain::{
    Error, ProviderStartRequest,
    batch::StartBatchRequest,
    catalog::{AccountRecord, AccountStatus},
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
    task::JoinHandle,
};

const KNOWN: &str = include_str!("fixtures/quota-client-v1/snapshot-v2.json");
const UNKNOWN: &str = include_str!("fixtures/quota-client-v1/unknown-snapshot-v2.json");
const EXHAUSTED: &str = include_str!("fixtures/quota-client-v1/exhausted-snapshot-v2.json");

/// Fake local Quota response behavior; no collector or credential is involved.
#[derive(Clone, Copy)]
enum ReplyMode {
    Known,
    Malformed,
    InvalidSchema,
    Exhausted,
    FutureClock,
    Oversize,
    Timeout,
    #[cfg(feature = "test-fixtures")]
    ShortValidity,
    #[cfg(feature = "test-fixtures")]
    MixedLatch,
    #[cfg(feature = "test-fixtures")]
    MixedLatchLong,
}

impl ReplyMode {
    /// Returns a synthetic state lifetime, or no response for malformed mode.
    fn lifetime(self) -> Option<f64> {
        match self {
            Self::Known | Self::InvalidSchema | Self::Exhausted | Self::FutureClock => Some(120.0),
            Self::Malformed | Self::Oversize | Self::Timeout => None,
            #[cfg(feature = "test-fixtures")]
            Self::ShortValidity | Self::MixedLatch | Self::MixedLatchLong => Some(1.0),
        }
    }
}

/// One fake service socket and its observed requests; owned only by the test.
struct FakeQuota {
    _dir: TempDir,
    path: PathBuf,
    task: JoinHandle<()>,
    requests: Arc<Mutex<Vec<Value>>>,
    deadlines: Arc<Mutex<Vec<f64>>>,
}

impl FakeQuota {
    /// Serves independent snapshot reads for a synthetic account pair.
    fn start(mode: ReplyMode) -> Self {
        let base = std::env::var_os("AGENT_RUN_TEST_SOCKET_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let dir = tempfile::Builder::new()
            .prefix("qa-prod-")
            .tempdir_in(base)
            .unwrap();
        let path = dir.path().join("quota.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let deadlines = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let times = deadlines.clone();
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = Vec::new();
                if stream.read_to_end(&mut request).await.is_err() {
                    continue;
                }
                if let Ok(value) = serde_json::from_slice(&request) {
                    seen.lock().unwrap().push(value);
                }
                if matches!(mode, ReplyMode::Timeout) {
                    std::future::pending::<()>().await;
                }
                let bytes = match mode.lifetime() {
                    None if matches!(mode, ReplyMode::Oversize) => vec![b'x'; 4 * 1024 * 1024 + 1],
                    None => b"{invalid}\n".to_vec(),
                    Some(lifetime) => {
                        let at = agent_run_domain::domain::now()
                            + if matches!(mode, ReplyMode::FutureClock) {
                                60.0
                            } else {
                                0.0
                            };
                        let until = at + lifetime;
                        times.lock().unwrap().push(until);
                        let mut value = if matches!(mode, ReplyMode::Exhausted) {
                            exhausted_response(at, until)
                        } else {
                            known_response(at, until)
                        };
                        #[cfg(feature = "test-fixtures")]
                        if matches!(mode, ReplyMode::MixedLatch | ReplyMode::MixedLatchLong) {
                            value = mixed_latch_response(
                                at,
                                until,
                                matches!(mode, ReplyMode::MixedLatchLong),
                            );
                        }
                        if matches!(mode, ReplyMode::InvalidSchema) {
                            value["result"]["schema_version"] = json!(1);
                        }
                        let mut bytes = serde_json::to_vec(&value).unwrap();
                        bytes.push(b'\n');
                        bytes
                    }
                };
                let _ = stream.write_all(&bytes).await;
            }
        });
        Self {
            _dir: dir,
            path,
            task,
            requests,
            deadlines,
        }
    }

    /// Stops only this fake listener and waits for its task to finish.
    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

/// One valid v2 response with unknown A and known B, at a controlled decision clock.
fn known_response(at: f64, until: f64) -> Value {
    let mut base: Value = serde_json::from_str(UNKNOWN).unwrap();
    let a: Value = serde_json::from_str(&UNKNOWN.replace("acct-test", "acct-a")).unwrap();
    let b: Value = serde_json::from_str(&KNOWN.replace("acct-test", "acct-b")).unwrap();
    let mut a = a["result"]["accounts"][0].clone();
    let mut b = b["result"]["accounts"][0].clone();
    for account in [&mut a, &mut b] {
        account["models"][0]["model"] = json!("m");
        account["models"][0]["native_model"] = json!("native-m");
    }
    b["models"][0]["state_validity"] = json!({"kind":"until","at":until});
    b["models"][0]["reset_at"] = json!(until + 1000.0);
    b["models"][0]["newest_observed_at"] = json!(at);
    b["windows"][0]["models"] = json!(["native-m"]);
    b["windows"][0]["observed_at"] = json!(at);
    b["windows"][0]["valid_until"] = json!(until);
    b["windows"][0]["last_reset_at"] = json!(until + 1000.0);
    b["windows"][0]["forecast"]["observed_at"] = json!(at);
    b["windows"][0]["forecast"]["reset_at"] = json!(until + 1000.0);
    b["windows"][0]["evaluation"]["reset_at"] = json!(until + 1000.0);
    b["advice"][0]["reset_at"] = json!(until + 1000.0);
    base["result"]["evaluated_at"] = json!(at);
    base["result"]["accounts"] = json!([a, b]);
    base["result"]["coverage"]["last_collection"]["observed_at"] = json!(at);
    base
}

/// A valid response with two independently exhausted synthetic accounts.
fn exhausted_response(at: f64, until: f64) -> Value {
    let mut base: Value = serde_json::from_str(EXHAUSTED).unwrap();
    let mut accounts = Vec::new();
    for id in ["acct-a", "acct-b"] {
        let source: Value = serde_json::from_str(&EXHAUSTED.replace("acct-test", id)).unwrap();
        let mut account = source["result"]["accounts"][0].clone();
        account["models"][0]["model"] = json!("m");
        account["models"][0]["native_model"] = json!("native-m");
        account["models"][0]["state_validity"] = json!({"kind":"until", "at":until});
        account["models"][0]["reset_at"] = json!(until);
        account["models"][0]["exhausted_until"] = json!(until);
        account["models"][0]["newest_observed_at"] = json!(at);
        account["windows"][0]["models"] = json!(["native-m"]);
        account["windows"][0]["observed_at"] = json!(at);
        account["windows"][0]["valid_until"] = json!(until);
        account["windows"][0]["last_reset_at"] = json!(until);
        account["windows"][0]["forecast"]["observed_at"] = json!(at);
        account["windows"][0]["forecast"]["reset_at"] = json!(until);
        account["windows"][0]["evaluation"]["reset_at"] = json!(until);
        account["latches"][0]["models"] = json!(["native-m"]);
        account["latches"][0]["observed_at"] = json!(at);
        account["latches"][0]["reset_at"] = json!(until);
        account["advice"][0]["reset_at"] = json!(until);
        accounts.push(account);
    }
    base["result"]["evaluated_at"] = json!(at);
    base["result"]["accounts"] = json!(accounts);
    base["result"]["coverage"]["last_collection"]["observed_at"] = json!(at);
    base
}

/// Combines a no-reset exhausted A with a soon-expiring known B and optional long-lived C.
#[cfg(feature = "test-fixtures")]
fn mixed_latch_response(at: f64, until: f64, add_long: bool) -> Value {
    let mut value = known_response(at, until);
    let mut exhausted = exhausted_response(at, until)["result"]["accounts"][0].clone();
    exhausted["models"][0]["state_validity"] = json!({"kind":"latched_no_reset"});
    exhausted["models"][0]["reset_at"] = Value::Null;
    exhausted["models"][0]["exhausted_until"] = Value::Null;
    exhausted["latches"][0]["reset_at"] = Value::Null;
    value["result"]["accounts"][0] = exhausted;
    if add_long {
        let b = serde_json::to_string(&value["result"]["accounts"][1]).unwrap();
        let mut c: Value = serde_json::from_str(&b.replace("acct-b", "acct-c")).unwrap();
        let long_until = at + 120.0;
        c["models"][0]["state_validity"]["at"] = json!(long_until);
        c["models"][0]["reset_at"] = json!(long_until + 1000.0);
        c["windows"][0]["valid_until"] = json!(long_until);
        c["windows"][0]["last_reset_at"] = json!(long_until + 1000.0);
        c["windows"][0]["forecast"]["reset_at"] = json!(long_until + 1000.0);
        c["windows"][0]["evaluation"]["reset_at"] = json!(long_until + 1000.0);
        c["advice"][0]["reset_at"] = json!(long_until + 1000.0);
        value["result"]["accounts"].as_array_mut().unwrap().push(c);
    }
    value
}

/// A private Run home with fake account references and an optional Quota socket.
fn home(socket: Option<&Path>, cap: usize) -> TempDir {
    let base = std::env::var_os("AGENT_RUN_TEST_TMP")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = tempfile::Builder::new()
        .prefix("quota-admission-v2-")
        .tempdir_in(base)
        .unwrap();
    agent_run_store::fixture::initialize(dir.path()).unwrap();
    std::fs::create_dir_all(dir.path().join("profiles")).unwrap();
    std::fs::write(dir.path().join("profiles/review.md"), "+++\nrevision='1'\nwrite=false\nnetwork=false\nallow_external_read_roots=false\nskills=[]\nmcp=[]\nrequired_constraints=[]\n+++\nReview.\n").unwrap();
    write_config(dir.path(), socket, cap);
    let mut store = agent_run_store::fixture::open(dir.path()).unwrap();
    for account in ["acct-a", "acct-b"] {
        store
            .register_account(&AccountRecord {
                account_id: account.parse().unwrap(),
                auth_family: "openai".parse().unwrap(),
                secret_ref: format!("named:codex:{account}").parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
    }
    dir
}

/// Replaces only this test home's nonsecret configuration.
fn write_config(home: &Path, socket: Option<&Path>, cap: usize) {
    let quota = socket.map_or_else(String::new, |socket| {
        format!(
            "[capacity.quota]\nsocket={}\n",
            toml::Value::String(socket.to_string_lossy().into_owned())
        )
    });
    let text = format!(
        "schema_version=2\n[core]\nmax_active_agents={cap}\n{quota}[harnesses.codex]\nbinary='/usr/bin/true'\nhome={}\n[harnesses.claude-code]\nbinary='/usr/bin/true'\nhome={}\n[providers.codex]\nharness='codex'\nconnection={{kind='native'}}\nauth_family='openai'\nlimits_source='none'\n[[providers.codex.models]]\nid='m'\nnative_model='native-m'\n[[providers.codex.bindings]]\nlabel='alpha'\naccount='acct-a'\n[[providers.codex.bindings]]\nlabel='beta'\naccount='acct-b'\n",
        toml::Value::String(home.join("codex").to_string_lossy().into_owned()),
        toml::Value::String(home.join("claude").to_string_lossy().into_owned())
    );
    std::fs::write(home.join("config.toml"), text).unwrap();
}

/// Builds a new idempotent provider request without launching an engine.
fn request(home: &Path, id: &str) -> ProviderStartRequest {
    serde_json::from_value(json!({"provider":"codex", "model":"m", "profile":"review", "task":"synthetic", "workdir":home, "request_id":id})).unwrap()
}

/// Reads the selected account from the exact newly admitted attempt.
fn selected(home: &Path, agent: &str) -> String {
    agent_run_store::fixture::open(home).unwrap().conn.query_row(
        "SELECT selected_account_id FROM attempts WHERE agent_id=? ORDER BY number DESC LIMIT 1",
        [agent], |row| row.get(0),
    ).unwrap()
}

/// Waits for the exact fake deadline before submitting a decision to the writer.
#[cfg(feature = "test-fixtures")]
fn pass_deadline(times: &Mutex<Vec<f64>>, attempt: u32) -> agent_run_domain::Result<()> {
    let deadline = times.lock().unwrap().get(attempt as usize).copied();
    if let Some(deadline) = deadline {
        loop {
            let remaining = deadline - agent_run_domain::domain::now();
            if remaining <= 0.0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs_f64(remaining.min(0.05)));
        }
    }
    Ok(())
}

#[test]
fn quota_admission_v2_absent_endpoint_preserves_frozen_config_serialization() {
    let home = home(None, 8);
    let (config, _) = agent_run_config::provider_config::ProviderConfig::load(home.path()).unwrap();
    assert!(config.capacity.quota.is_none());
    let capacity = serde_json::to_value(&config.capacity).unwrap();
    assert!(capacity.get("quota").is_none());
    let document = serde_json::to_value(&config).unwrap();
    let restored: agent_run_config::provider_config::ProviderConfig =
        serde_json::from_value(document.clone()).unwrap();
    assert_eq!(serde_json::to_value(&restored).unwrap(), document);
    assert_eq!(restored.snapshot().unwrap(), config.snapshot().unwrap());
}

#[tokio::test]
async fn quota_admission_v2_unconfigured_endpoint_does_not_use_old_latches() {
    let home = home(None, 8);
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    store.conn.execute("INSERT INTO quota_exhaustion(account_id,quota_key,source,window_id,observed_at,reset_at) VALUES('acct-a','acct-a::primary','old','hour',1,4000000000)", []).unwrap();
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let result = service
        .admit_provider(request(home.path(), "unconfigured"))
        .await
        .unwrap();
    assert_eq!(
        selected(home.path(), result["agent_id"].as_str().unwrap()),
        "acct-a"
    );
}

#[tokio::test]
async fn quota_admission_v2_known_ignores_old_latch_and_replays_when_down() {
    let fake = FakeQuota::start(ReplyMode::Known);
    let home = home(Some(&fake.path), 8);
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    store.conn.execute("INSERT INTO quota_exhaustion(account_id,quota_key,source,window_id,observed_at,reset_at) VALUES('acct-b','acct-b::primary','old','hour',1,4000000000)", []).unwrap();
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let input = request(home.path(), "known-and-replay");
    let first = service.admit_provider(input.clone()).await.unwrap();
    let id = first["agent_id"].as_str().unwrap();
    assert_eq!(selected(home.path(), id), "acct-b");
    let metadata: String = store
        .conn
        .query_row(
            "SELECT data_json FROM events WHERE agent_id=? AND kind='start_accepted'",
            [id],
            |row| row.get(0),
        )
        .unwrap();
    let metadata: Value = serde_json::from_str(&metadata).unwrap();
    assert_eq!(metadata["quota_decision"]["revision"], 1);
    assert_eq!(fake.requests.lock().unwrap().len(), 1);
    assert_eq!(fake.deadlines.lock().unwrap().len(), 1);
    fake.stop().await;
    let replay = service.admit_provider(input).await.unwrap();
    assert_eq!(replay["created"], false);
    assert_eq!(replay["agent_id"], first["agent_id"]);
    let unknown = service
        .admit_provider(request(home.path(), "unknown-after-down"))
        .await
        .unwrap();
    assert_eq!(
        selected(home.path(), unknown["agent_id"].as_str().unwrap()),
        "acct-a"
    );
}

#[tokio::test]
async fn quota_admission_v2_malformed_response_becomes_unknown() {
    for mode in [
        ReplyMode::Malformed,
        ReplyMode::InvalidSchema,
        ReplyMode::Oversize,
    ] {
        let fake = FakeQuota::start(mode);
        let home = home(Some(&fake.path), 8);
        let writer = common::writer::Harness::start(home.path());
        let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
        let result = service
            .admit_provider(request(home.path(), "malformed-unknown"))
            .await
            .unwrap();
        assert_eq!(
            selected(home.path(), result["agent_id"].as_str().unwrap()),
            "acct-a"
        );
        assert_eq!(fake.requests.lock().unwrap().len(), 1);
        fake.stop().await;
    }
}

#[tokio::test]
async fn quota_admission_v2_wrong_expected_uid_is_unknown_before_request() {
    let fake = FakeQuota::start(ReplyMode::Known);
    let home = home(Some(&fake.path), 8);
    let path = home.path().join("config.toml");
    let mut config: toml::Value = toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    // SAFETY: geteuid is a read-only process identity query.
    let wrong_uid = unsafe { libc::geteuid() } ^ 1;
    config["capacity"]["quota"].as_table_mut().unwrap().insert(
        "expected_service_uid".into(),
        toml::Value::Integer(i64::from(wrong_uid)),
    );
    std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let result = service
        .admit_provider(request(home.path(), "wrong-peer-unknown"))
        .await
        .unwrap();
    assert_eq!(
        selected(home.path(), result["agent_id"].as_str().unwrap()),
        "acct-a"
    );
    assert!(fake.requests.lock().unwrap().is_empty());
    fake.stop().await;
}

#[tokio::test]
async fn quota_admission_v2_clock_before_evaluation_is_unknown() {
    let fake = FakeQuota::start(ReplyMode::FutureClock);
    let home = home(Some(&fake.path), 8);
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let result = service
        .admit_provider(request(home.path(), "clock-rollback"))
        .await
        .unwrap();
    assert_eq!(
        selected(home.path(), result["agent_id"].as_str().unwrap()),
        "acct-a"
    );
    assert_eq!(fake.requests.lock().unwrap().len(), 1);
    fake.stop().await;
}

#[tokio::test]
async fn quota_admission_v2_timeout_is_unknown_without_blocking_executor() {
    let fake = FakeQuota::start(ReplyMode::Timeout);
    let home = home(Some(&fake.path), 8);
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let started = std::time::Instant::now();
    let call = service.admit_provider(request(home.path(), "timeout-unknown"));
    tokio::pin!(call);
    tokio::select! {
        result = &mut call => panic!("quota wait ended early: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {},
    }
    let result = tokio::time::timeout(std::time::Duration::from_secs(7), &mut call)
        .await
        .unwrap()
        .unwrap();
    assert!(started.elapsed() >= std::time::Duration::from_secs(5));
    assert_eq!(
        selected(home.path(), result["agent_id"].as_str().unwrap()),
        "acct-a"
    );
    assert_eq!(fake.requests.lock().unwrap().len(), 1);
    fake.stop().await;
}

#[tokio::test]
async fn quota_admission_v2_exhausted_never_becomes_unknown_before_deadline() {
    let fake = FakeQuota::start(ReplyMode::Exhausted);
    let home = home(Some(&fake.path), 8);
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let result = service
        .admit_provider(request(home.path(), "exhausted"))
        .await;
    assert!(matches!(
        result,
        Err(Error::QuotaAdmission(
            agent_run_domain::catalog::QuotaAdmissionError::QuotaExhausted { .. }
        ))
    ));
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let count: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(fake.requests.lock().unwrap().len(), 1);
    fake.stop().await;
}

#[tokio::test]
async fn quota_admission_v2_batch_uses_one_snapshot_and_rolls_back() {
    let fake = FakeQuota::start(ReplyMode::Known);
    let home = home(Some(&fake.path), 1);
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let input: StartBatchRequest =
        serde_json::from_value(json!({"request_id":"quota-batch", "jobs":[
            {"provider":"codex","model":"m","profile":"review","task":"one","workdir":home.path()},
            {"provider":"codex","model":"m","profile":"review","task":"two","workdir":home.path()}
        ]}))
        .unwrap();
    assert!(matches!(
        service.admit_batch(input.clone()).await,
        Err(Error::Capacity)
    ));
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let count: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(fake.requests.lock().unwrap().len(), 1);
    write_config(home.path(), Some(&fake.path), 8);
    let result = service.admit_batch(input.clone()).await.unwrap();
    assert!(!result.replayed);
    assert_eq!(fake.requests.lock().unwrap().len(), 2);
    for request in fake.requests.lock().unwrap().iter() {
        assert_eq!(request["version"], 1);
        assert_eq!(request["method"], "snapshot");
        assert_eq!(request["accounts"], json!([]));
        assert!(request.get("model").is_none());
    }
    fake.stop().await;
    let replay = service.admit_batch(input).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, result.receipt);
}

#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_admission_v2_local_stale_refetches_one_full_batch() {
    let fake = FakeQuota::start(ReplyMode::Known);
    let home = home(Some(&fake.path), 8);
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let input: StartBatchRequest =
        serde_json::from_value(json!({"request_id":"quota-retry", "jobs":[
            {"provider":"codex","model":"m","profile":"review","task":"one","workdir":home.path()},
            {"provider":"codex","model":"m","profile":"review","task":"two","workdir":home.path()}
        ]}))
        .unwrap();
    let mut hook = |attempt| {
        if attempt == 0 {
            let store = agent_run_store::fixture::open(home.path())?;
            store.conn.execute(
                "UPDATE quota_capacity_revision SET revision=revision+1 WHERE id=1",
                [],
            )?;
        }
        Ok(())
    };
    let result = service
        .admit_batch_observed(input, &mut hook)
        .await
        .unwrap();
    assert!(!result.replayed);
    assert_eq!(result.receipt.jobs.len(), 2);
    assert_eq!(fake.requests.lock().unwrap().len(), 2);
    fake.stop().await;
}

#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_admission_v2_expired_writer_retries_then_uses_unknown() {
    let fake = FakeQuota::start(ReplyMode::ShortValidity);
    let home = home(Some(&fake.path), 8);
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let times = fake.deadlines.clone();
    let mut hook = |attempt| pass_deadline(&times, attempt);
    let admitted = service
        .admit_provider_observed(request(home.path(), "expiry-fallback"), &mut hook)
        .await
        .unwrap();
    assert_eq!(
        selected(home.path(), admitted["agent_id"].as_str().unwrap()),
        "acct-a"
    );
    assert_eq!(fake.requests.lock().unwrap().len(), 4);
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let metadata: String = store
        .conn
        .query_row(
            "SELECT data_json FROM events WHERE agent_id=? AND kind='start_accepted'",
            [admitted["agent_id"].as_str().unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&metadata).unwrap()["quota_decision"]["revision"],
        1
    );
    fake.stop().await;
}

/// Forces each external retry past B's deadline; C, when present, remains fresh.
#[cfg(feature = "test-fixtures")]
async fn mixed_latch_fallback(add_long: bool) {
    let fake = FakeQuota::start(if add_long {
        ReplyMode::MixedLatchLong
    } else {
        ReplyMode::MixedLatch
    });
    let home = home(Some(&fake.path), 8);
    if add_long {
        let path = home.path().join("config.toml");
        let mut config = std::fs::read_to_string(&path).unwrap();
        config.push_str("\n[[providers.codex.bindings]]\nlabel='gamma'\naccount='acct-c'\n");
        std::fs::write(path, config).unwrap();
        let mut store = agent_run_store::fixture::open(home.path()).unwrap();
        store
            .register_account(&AccountRecord {
                account_id: "acct-c".parse().unwrap(),
                auth_family: "openai".parse().unwrap(),
                secret_ref: "named:codex:acct-c".parse().unwrap(),
                status: AccountStatus::Enabled,
            })
            .unwrap();
    }
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let times = fake.deadlines.clone();
    let mut hook = |attempt| pass_deadline(&times, attempt);
    let admitted = service
        .admit_provider_observed(request(home.path(), "mixed-latch"), &mut hook)
        .await
        .unwrap();
    assert_eq!(
        selected(home.path(), admitted["agent_id"].as_str().unwrap()),
        if add_long { "acct-c" } else { "acct-b" }
    );
    assert_eq!(fake.requests.lock().unwrap().len(), 4);
    fake.stop().await;
}

#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_admission_v2_expired_peer_does_not_erase_no_reset_latch() {
    mixed_latch_fallback(false).await;
}

#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_admission_v2_expired_peer_does_not_erase_other_live_class() {
    mixed_latch_fallback(true).await;
}

#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_admission_v2_batch_expiry_keeps_shared_no_reset_latch() {
    let fake = FakeQuota::start(ReplyMode::MixedLatch);
    let home = home(Some(&fake.path), 8);
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let input: StartBatchRequest =
        serde_json::from_value(json!({"request_id":"mixed-latch-batch", "jobs":[
            {"provider":"codex","model":"m","profile":"review","task":"one","workdir":home.path()},
            {"provider":"codex","model":"m","profile":"review","task":"two","workdir":home.path()}
        ]}))
        .unwrap();
    let times = fake.deadlines.clone();
    let mut hook = |attempt| pass_deadline(&times, attempt);
    let result = service
        .admit_batch_observed(input, &mut hook)
        .await
        .unwrap();
    assert_eq!(result.receipt.jobs.len(), 2);
    for job in &result.receipt.jobs {
        assert_eq!(selected(home.path(), job.agent_id.as_str()), "acct-b");
    }
    assert_eq!(fake.requests.lock().unwrap().len(), 4);
    fake.stop().await;
}

/// Simulates a writer seeing a rolled-back clock after a valid source decision.
#[cfg(feature = "test-fixtures")]
fn simulate_writer_clock_rollback(candidate: &mut agent_run_domain::catalog::QuotaCandidateSet) {
    if let Some(decision) = &mut candidate.quota_decision {
        decision.evaluated_at = agent_run_domain::domain::now() + 60.0;
        if decision.valid_until.is_some() {
            decision.valid_until = Some(decision.evaluated_at + 60.0);
        }
    }
}

#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_admission_v2_clock_rollback_stays_invalid_after_restore() {
    let fake = FakeQuota::start(ReplyMode::MixedLatch);
    let home = home(Some(&fake.path), 8);
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let mut hook = |_: u32, candidate: &mut agent_run_domain::catalog::QuotaCandidateSet| {
        simulate_writer_clock_rollback(candidate);
        Ok(())
    };
    let admitted = service
        .admit_provider_decision_observed(request(home.path(), "sticky-rollback"), &mut hook)
        .await
        .unwrap();
    // After the simulated clock restores, A must be unknown rather than reused as exhausted.
    assert_eq!(
        selected(home.path(), admitted["agent_id"].as_str().unwrap()),
        "acct-a"
    );
    assert_eq!(fake.requests.lock().unwrap().len(), 4);
    let store = agent_run_store::fixture::open(home.path()).unwrap();
    let metadata: String = store
        .conn
        .query_row(
            "SELECT data_json FROM events WHERE agent_id=? AND kind='start_accepted'",
            [admitted["agent_id"].as_str().unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        serde_json::from_str::<Value>(&metadata)
            .unwrap()
            .get("quota_decision")
            .is_none()
    );
    fake.stop().await;
}

#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_admission_v2_batch_rollback_invalidates_every_job() {
    let fake = FakeQuota::start(ReplyMode::MixedLatch);
    let home = home(Some(&fake.path), 8);
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let input: StartBatchRequest =
        serde_json::from_value(json!({"request_id":"sticky-rollback-batch", "jobs":[
            {"provider":"codex","model":"m","profile":"review","task":"one","workdir":home.path()},
            {"provider":"codex","model":"m","profile":"review","task":"two","workdir":home.path()}
        ]}))
        .unwrap();
    let mut hook = |_: u32, jobs: &mut [agent_run_store::ops::BatchMemberSpec]| {
        for job in jobs {
            simulate_writer_clock_rollback(&mut job.inputs.candidates);
        }
        Ok(())
    };
    let admitted = service
        .admit_batch_decision_observed(input, &mut hook)
        .await
        .unwrap();
    let accounts: Vec<_> = admitted
        .receipt
        .jobs
        .iter()
        .map(|job| selected(home.path(), job.agent_id.as_str()))
        .collect();
    assert_eq!(accounts, ["acct-a", "acct-b"]);
    assert_eq!(fake.requests.lock().unwrap().len(), 4);
    fake.stop().await;
}

#[cfg(feature = "test-fixtures")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quota_admission_v2_invalid_writer_reference_is_not_reused() {
    let fake = FakeQuota::start(ReplyMode::MixedLatch);
    let home = home(Some(&fake.path), 8);
    let writer = common::writer::Harness::start(home.path());
    let service = Service::new(home.path().to_path_buf()).with_writer(writer.broker());
    let mut hook = |_: u32, candidate: &mut agent_run_domain::catalog::QuotaCandidateSet| {
        if let Some(decision) = &mut candidate.quota_decision {
            decision.source_epoch = "invalid".into();
        }
        Ok(())
    };
    let admitted = service
        .admit_provider_decision_observed(request(home.path(), "invalid-reference"), &mut hook)
        .await
        .unwrap();
    assert_eq!(
        selected(home.path(), admitted["agent_id"].as_str().unwrap()),
        "acct-a"
    );
    assert_eq!(fake.requests.lock().unwrap().len(), 4);
    fake.stop().await;
}
